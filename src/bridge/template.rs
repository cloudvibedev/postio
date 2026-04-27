use std::collections::BTreeMap;

use serde_json::Value;

#[derive(Debug, Clone)]
pub struct TemplateContext {
    pub params: BTreeMap<String, String>,
    pub query: BTreeMap<String, String>,
    pub headers: BTreeMap<String, String>,
    pub body: Value,
    pub context: BTreeMap<String, String>,
}

pub fn render_value(value: &Value, ctx: &TemplateContext) -> Value {
    match value {
        Value::String(value) => render_string(value, ctx),
        Value::Array(values) => {
            Value::Array(values.iter().map(|item| render_value(item, ctx)).collect())
        }
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), render_value(value, ctx)))
                .collect(),
        ),
        value => value.clone(),
    }
}

pub fn render_string(value: &str, ctx: &TemplateContext) -> Value {
    let trimmed = value.trim();
    if let Some(expression) = whole_template_expression(trimmed) {
        return lookup(expression, ctx).unwrap_or(Value::Null);
    }

    let mut rendered = String::new();
    let mut rest = value;
    while let Some(start) = rest.find("{{") {
        let (before, after_start) = rest.split_at(start);
        rendered.push_str(before);
        if let Some(end) = after_start.find("}}") {
            let expression = after_start[2..end].trim();
            let replacement = lookup(expression, ctx)
                .map(|value| match value {
                    Value::String(value) => value,
                    other => other.to_string(),
                })
                .unwrap_or_default();
            rendered.push_str(&replacement);
            rest = &after_start[end + 2..];
        } else {
            rendered.push_str(after_start);
            rest = "";
        }
    }
    rendered.push_str(rest);
    Value::String(rendered)
}

pub fn render_to_string(value: &str, ctx: &TemplateContext) -> String {
    match render_string(value, ctx) {
        Value::String(value) => value,
        value => value.to_string(),
    }
}

fn whole_template_expression(value: &str) -> Option<&str> {
    value
        .strip_prefix("{{")
        .and_then(|value| value.strip_suffix("}}"))
        .map(str::trim)
}

fn lookup(expression: &str, ctx: &TemplateContext) -> Option<Value> {
    let mut parts = expression.split('.');
    let root = parts.next()?;
    match root {
        "params" => lookup_string_map(&ctx.params, parts),
        "query" => lookup_string_map(&ctx.query, parts),
        "headers" => lookup_string_map(&ctx.headers, parts),
        "context" => lookup_string_map(&ctx.context, parts),
        "body" => lookup_json(&ctx.body, parts),
        _ => None,
    }
}

fn lookup_string_map<'a>(
    map: &BTreeMap<String, String>,
    mut parts: impl Iterator<Item = &'a str>,
) -> Option<Value> {
    let key = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    map.get(key).cloned().map(Value::String)
}

fn lookup_json<'a>(value: &Value, parts: impl Iterator<Item = &'a str>) -> Option<Value> {
    let mut current = value;
    for part in parts {
        current = current.get(part)?;
    }
    Some(current.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_params_and_body() {
        let ctx = TemplateContext {
            params: BTreeMap::from([("name".to_string(), "events".to_string())]),
            query: BTreeMap::new(),
            headers: BTreeMap::new(),
            body: serde_json::json!({"id": 42}),
            context: BTreeMap::new(),
        };

        assert_eq!(
            render_to_string("topic-{{ params.name }}", &ctx),
            "topic-events"
        );
        assert_eq!(render_string("{{ body.id }}", &ctx), serde_json::json!(42));
    }
}
