#[allow(dead_code)]
pub struct AisMultipartInfo {
    pub total_fragments: u8,
    pub fragment_number: u8,
    pub sequential_id: String,
    pub channel: String,
}

pub fn parse_ais_multipart(line: &str) -> Option<AisMultipartInfo> {
    let line = line.trim_end_matches(|c| c == '\r' || c == '\n');

    if !line.starts_with('!') {
        return None;
    }

    let body = line.strip_prefix('!')?;
    let parts: Vec<&str> = body.split(',').collect();
    if parts.len() < 7 {
        return None;
    }

    let sentence = parts[0];
    if !sentence.ends_with("VDM") && !sentence.ends_with("VDO") {
        return None;
    }

    Some(AisMultipartInfo {
        total_fragments: parts[1].parse().unwrap_or(1),
        fragment_number: parts[2].parse().unwrap_or(1),
        sequential_id: parts[3].to_string(),
        channel: parts[4].to_string(),
    })
}

pub fn compute_affinity_key(info: &AisMultipartInfo) -> Option<String> {
    if info.sequential_id.is_empty() || info.total_fragments <= 1 {
        return None;
    }
    Some(format!("{}{}", info.sequential_id, info.channel))
}
