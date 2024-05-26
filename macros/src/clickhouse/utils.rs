use std::{env, fs, io, io::BufRead, path};

use syn::LitStr;

pub(crate) fn find_file_path(table_name: &str, database_name: &str, table_path: Option<&LitStr>) -> String {
    dotenv::dotenv().ok();
    let root_path = workspace_dir();
    let path = root_path.to_str().unwrap();

    let clickhouse_table_dir = if let Some(tp) = table_path {
        format!("{path}/{}", tp.value())
    } else {
        return "".to_string();
    };

    let mut pathsss = Vec::new();
    if let Ok(entries) = visit_dirs(path::Path::new(&clickhouse_table_dir)) {
        for entry in entries {
            let entry_path = entry.path();
            if let Ok(lines) = read_lines(&entry_path) {
                for (i, ln) in lines.map_while(Result::ok).enumerate() {
                    if i == 0 {
                        pathsss.push((entry_path.clone(), ln.clone()));
                    }
                    if check_line(ln, table_name, database_name) {
                        return entry.path().to_str().unwrap().to_string();
                    }
                }
            }
        }
    }

    panic!(
        "NO FILE FOUND FOR DATABASE: {database_name}, TABLE: {table_name} for in directory: {clickhouse_table_dir}\n\nPATHS SEARCHED {:?}",
        pathsss
    );
}
pub(crate) fn workspace_dir() -> path::PathBuf {
    let output = std::process::Command::new(env!("CARGO"))
        .arg("locate-project")
        .arg("--workspace")
        .arg("--message-format=plain")
        .output()
        .unwrap()
        .stdout;
    let cargo_path = path::Path::new(std::str::from_utf8(&output).unwrap().trim());
    cargo_path.parent().unwrap().to_path_buf()
}

/// Recursive directory traversal
fn visit_dirs(dir: &path::Path) -> io::Result<Vec<fs::DirEntry>> {
    let mut entries = Vec::new();

    if dir.is_dir() {
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            if path.is_dir() {
                entries.append(&mut visit_dirs(&path).unwrap());
            } else {
                entries.push(entry);
            }
        }
    }

    Ok(entries)
}

fn read_lines<P>(filename: P) -> io::Result<io::Lines<io::BufReader<fs::File>>>
where
    P: AsRef<path::Path>
{
    let file = fs::File::open(filename)?;
    Ok(io::BufReader::new(file).lines())
}

fn check_line(ln: String, table_name: &str, database_name: &str) -> bool {
    let formatted = ln.contains(&format!("{}.{} ", database_name, table_name.to_lowercase()));
    let remote = ln.contains(&format!("{}.{}_remote ", database_name, table_name.to_lowercase()));

    let create = ln.contains("CREATE");

    (formatted || remote) && create
}
