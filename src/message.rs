use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct Message {
    pub data: Bytes,
    pub affinity_key: Option<String>,
}
