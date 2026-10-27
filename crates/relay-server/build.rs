// `sqlx::migrate!` embeds the migration files at compile time; make sure a new
// or edited migration triggers a rebuild.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
