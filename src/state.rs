#[derive(Clone)]
pub struct AppState;

impl AppState {
    pub fn new() -> Self {
        Self
    }
}

#[cfg(test)]
mod tests {
    use super::AppState;

    #[test]
    fn state_can_be_constructed() {
        let _state = AppState::new();
    }
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}
