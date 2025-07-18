use env_logger;

/// Initializes env_logger for tests automatically.
#[ctor::ctor]
fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}
