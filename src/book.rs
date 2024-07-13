use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Book {
    pub id: u32,
    pub idx: u32,
    pub name: String,
    pub size: u32,
    pub added: String,
}
impl Book {
    pub fn new<T: Into<String>>(id: u32, idx: u32, name: T, size: u32, added: T) -> Self {
        Self {
            id,
            idx,
            name: name.into(),
            size,
            added: added.into(),
        }
    }
}
impl fmt::Display for Book {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        let size = format_size(self.size);
        if self.idx > 0 {
            write!(fmt, "{} {} ({}) [{size}]", self.idx, self.name, self.added)
        } else {
            write!(fmt, "{} ({}) [{size}]", self.name, self.added)
        }
    }
}

fn format_size(size: u32) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * KB;

    if size as f64 >= MB {
        format!("{:.2} MB", size as f64 / MB)
    } else if size as f64 >= KB {
        format!("{:.2} KB", size as f64 / KB)
    } else {
        format!("{} B", size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size() {
        assert_eq!(format_size(156), String::from("156 B"));
        assert_eq!(format_size(2450), String::from("2.39 KB"));
        assert_eq!(format_size(4050000), String::from("3.86 MB"));
    }

    #[test]
    fn fmt() {
        assert_eq!(
            "1 A (2024-10-10) [42 B]",
            format!("{}", &Book::new(1, 1, "A", 42, "2024-10-10"))
        );
    }
}
