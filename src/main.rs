use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::env;
use brevduva::{SyncStorage, channel::SerializationFormat};
use hyper::server::conn::http1;
use machineid_rs::HWIDComponent;
use regex::Regex;
use tokio::net::TcpListener;
use tokio::task;
use hyper::{body::Incoming, Request, Response, Method};
use hyper_util::rt::TokioIo;
use hyper::service::Service;
use std::future::Future;
use std::pin::Pin;
use log::{error, info, warn};
use std::fmt::Write;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MetricKey {
    name: String,
    labels: BTreeMap<String, String>,
}

// Structure to hold metric values and labels
type MetricsMap = Arc<Mutex<HashMap<MetricKey, f64>>>;

// Returns: (mqtt_topic, Vec<(label_name, position_in_topic)>)
fn pattern_to_mqtt(topic: &str) -> (String, Vec<(regex::Regex, Vec<String>, usize)>) {
    let mut mqtt = String::new();
    let mut labels = Vec::new();
    for (i, part) in topic.split('/').enumerate() {
        if part.contains('<') && part.contains('>') {
            mqtt.push_str("/+");

            let re = regex::Regex::new(r"<[a-zA-Z0-9_]+>").unwrap();
            let mut part_regex = String::new();
            let mut last_index = 0;
            let mut group_names = vec![];
            for cap in re.find_iter(part) {
                part_regex += &part[last_index..cap.start()];
                last_index = cap.end();
                part_regex += "(.+)";
                let label = cap.as_str().trim_start_matches('<').trim_end_matches('>');
                group_names.push(label.to_string());
            }
            part_regex += &part[last_index..];

            labels.push((Regex::new(&format!("^{part_regex}$")).unwrap(), group_names, i));
            println!("Pattern part: {} -> regex: ^{}$ with labels {:?}", part, part_regex, labels.last().unwrap().1);
        } else if !part.is_empty() {
            mqtt.push('/');
            mqtt.push_str(part);
        }
    }
    (mqtt.trim_start_matches('/').to_string(), labels)
}

 struct MetricsService {
    metrics: MetricsMap,
}

impl Service<Request<Incoming>> for MetricsService {
type Response = Response<String>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let metrics_map = self.metrics.clone();
        Box::pin(async move {
            if req.method() == Method::GET && req.uri().path() == "/metrics" {
                let mut body = String::new();
                let metrics = metrics_map.lock().unwrap();
                for (key, value) in metrics.iter() {
                    body.push_str(&key.name);
                    if !key.labels.is_empty() {
                        body.push('{');
                        for (i, (k, v)) in key.labels.iter().enumerate() {
                            if i > 0 {
                                body.push(',');
                            }
                            write!(body, "{}=\"{}\"", k, v).unwrap();
                        }
                        body.push_str("}");
                    }
                    write!(body, " {}\n", value).unwrap();
                }
                Ok(Response::new(body))
            } else {
                Ok(Response::new("Not Found".into()))
            }
        })
    }
}

impl Clone for MetricsService {
    fn clone(&self) -> Self {
        MetricsService {
            metrics: self.metrics.clone(),
        }
    }
}

struct Topic {
    name: String,
    pattern: String,
    value_fn: fn(&str) -> anyhow::Result<f64>,
}

struct TopicBuilder {
    name: Option<String>,
    pattern: Option<String>,
    value_fn: fn(&str) -> anyhow::Result<f64>,
}

impl TopicBuilder {
    fn new() -> Self {
        TopicBuilder {
            name: None,
            pattern: None,
            value_fn: |x| Ok(x.parse::<f64>()?),
        }
    }

    fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    fn pattern(mut self, pattern: impl Into<String>) -> Self {
        self.pattern = Some(pattern.into());
        self
    }

    fn value_fn(mut self, value_fn: fn(&str) -> anyhow::Result<f64>) -> Self {
        self.value_fn = value_fn;
        self
    }

    fn build(self) -> anyhow::Result<Topic> {
        Ok(Topic {
            name: self.name.ok_or_else(|| anyhow::anyhow!("Missing name"))?,
            pattern: self.pattern.ok_or_else(|| anyhow::anyhow!("Missing pattern"))?,
            value_fn: self.value_fn,
        })
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let machine_id = machineid_rs::IdBuilder::new(machineid_rs::Encryption::SHA256)
        .add_component(HWIDComponent::SystemID)
        .build("somekey")
        .unwrap()[0..12].to_string();

    // Example config: can be replaced with env/config parsing
    let mqtt_host = env::var("MQTT_HOST").unwrap_or_else(|_| "arongranberg.com".to_string());
    let mqtt_port = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".to_string());
    let mqtt_user = env::var("MQTT_USER").unwrap_or_else(|_| "wakeup_alarm".to_string());
    let mqtt_pass = env::var("MQTT_PASS").unwrap_or_else(|_| "xafzz25nomehasff".to_string());
    let topic_patterns: Vec<Topic> = vec![
        TopicBuilder::new()
            .name("device_status")
            .pattern("devices/<device_type> <device_id>/status")
            .value_fn(|s| if s == "\"online\"" { Ok(1.0) } else { Ok(0.0) })
            .build()?,
        TopicBuilder::new()
            .name("alarm_is_playing")
            .pattern("alarm/<device_type> <device_id>/is_playing")
            .value_fn(|s| if s.parse::<bool>()? { Ok(1.0) } else { Ok(0.0) })
            .build()?,
        TopicBuilder::new()
            .name("alarm_is_user_in_bed")
            .pattern("alarm/<device_type> <device_id>/is_user_in_bed")
            .value_fn(|s| if s.parse::<bool>()? { Ok(1.0) } else { Ok(0.0) })
            .build()?
    ];

    // Shared metrics map
    let metrics: MetricsMap = Arc::new(Mutex::new(HashMap::new()));

    // Spawn MQTT listener
    let metrics_mqtt = metrics.clone();
    let mqtt_task = task::spawn(async move {
        // Connect to MQTT broker using brevduva
        let storage = SyncStorage::new(
            &format!("mqtt-exporter {machine_id}"), // client_id
            &format!("mqtt://{}:{}", mqtt_host, mqtt_port),
            &mqtt_user,
            &mqtt_pass,
            brevduva::SessionPersistance::Transient,
        ).await;

        for Topic { name, pattern, value_fn } in topic_patterns {
            let (mqtt_topic, label_positions) = pattern_to_mqtt(&pattern);
            let channel = storage.add_channel::<String>(&mqtt_topic, SerializationFormat::String).await;
            if let Ok((_container, mut receiver)) = channel {
                let metrics_inner = metrics_mqtt.clone();
                let label_positions = label_positions.clone();
                info!("Subscribed to topic pattern: {} (MQTT topic: {})", pattern, mqtt_topic);
                task::spawn(async move {
                    while let Some(msg) = receiver.recv().await {
                        let topic_parts: Vec<_> = msg.topic.split('/').collect();
                        let mut labels = BTreeMap::new();
                        let mut parse_ok = true;
                        for (regex, group_names, pos) in &label_positions {
                            if let Some(val) = topic_parts.get(*pos) {
                                if let Some(captures) = regex.captures(val) {
                                    for (i, name) in group_names.iter().enumerate() {
                                        if let Some(m) = captures.get(i + 1) {
                                            labels.insert(name.clone(), m.as_str().to_string());
                                        } else {
                                            warn!("No match for group {} in value {}", name, val);
                                            parse_ok = false;
                                        }
                                    }
                                } else {
                                    warn!("{:?} did not match value '{}' for topic {}", regex, val, msg.topic);
                                    parse_ok = false;
                                }
                            }
                        }

                        if !parse_ok {
                            continue;
                        }

                        let mut metrics_map = metrics_inner.lock().unwrap();
                        let key = MetricKey { name: name.clone(), labels };

                        if msg.message.is_empty() {
                            warn!("Clearing {} with labels {:?}", name, key.labels);
                            metrics_map.remove(&key);
                            continue;
                        }

                        let value: f64 = match value_fn(&msg.message) {
                            Ok(v) => v,
                            Err(e) => {
                                warn!("Failed to parse value for {}: {}. Error: {:?}", name, msg.message, e);
                                continue;
                            }
                        };
                        info!("Received message on {}: {} with labels {:?}", name, value, key.labels);
                        metrics_map.insert(key, value);
                    }
                });
            }
        }

        storage.wait_for_sync().await;
        info!("Connected to MQTT broker at {}:{}", mqtt_host, mqtt_port);

        // Sleep indefinitely to keep the task alive
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(60)).await;
        }
    });

    // Spawn HTTP server for /metrics using hyper 1.7 API
    let metrics_http = metrics.clone();

    let http_task = task::spawn(async move {
        let addr = SocketAddr::from(([127, 0, 0, 1], 3001));

        // We create a TcpListener and bind it
        let listener = TcpListener::bind(addr).await.unwrap();

        let service = MetricsService { metrics: metrics_http };
        info!("Prometheus metrics available at http://{}:{}/metrics", addr.ip(), addr.port());

        loop {
            let (stream, _) = listener.accept().await.unwrap();

            // Use an adapter to access something implementing `tokio::io` traits as if they implement
            // `hyper::rt` IO traits.
            let io = TokioIo::new(stream);
            let service_clone = service.clone();

            // Spawn a tokio task to serve multiple connections concurrently
            tokio::task::spawn(async move {
                // Finally, we bind the incoming connection to our `hello` service
                if let Err(err) = http1::Builder::new()
                    // `service_fn` converts our function in a `Service`
                    .serve_connection(io, service_clone)
                    .await
                {
                    error!("Error serving connection: {:?}", err);
                }
            });

        }
    });

    // Wait for both tasks
    let _ = tokio::join!(mqtt_task, http_task);

    Ok(())
}
