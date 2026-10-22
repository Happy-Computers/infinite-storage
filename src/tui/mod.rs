use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    fs, io,
    path::{Component, Path, PathBuf},
};

const MAX_PREVIEW_BYTES: u64 = 1024 * 1024;

const CONTROLS: &str = "Up/Down: Navigate | Enter: Open | Backspace: Up | q: Quit";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FileKind {
    Folder,
    Image,
    Video,
    Text,
    Archive,
    Unknown,
}

impl FileKind {
    fn from_name(name: &str, is_dir: bool) -> Self {
        if is_dir {
            return Self::Folder;
        }

        let extension = Path::new(name)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();

        match extension.as_str() {
            "png" | "jpg" | "jpeg" | "gif" | "bmp" | "webp" | "tif" | "tiff" | "ico" | "avif" => {
                Self::Image
            }

            "mp4" | "mkv" | "mov" | "avi" | "webm" | "m4v" | "mpeg" | "mpg" | "wmv" | "3gp" => {
                Self::Video
            }

            "txt" | "md" | "rs" | "toml" | "json" | "yaml" | "yml" | "xml" | "html" | "css"
            | "js" | "ts" | "py" | "java" | "c" | "cpp" | "h" | "sh" | "ps1" | "sql" | "log"
            | "csv" | "ini" | "env" | "gitignore" => Self::Text,

            "zip" | "7z" | "rar" | "tar" | "gz" | "bz2" | "xz" | "zst" => Self::Archive,

            _ => Self::Unknown,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::Folder => "Folder",
            Self::Image => "Image",
            Self::Video => "Video",
            Self::Text => "Text file",
            Self::Archive => "Archive",
            Self::Unknown => "Unknown file",
        }
    }

    fn icon(self) -> &'static str {
        match self {
            Self::Folder => "[DIR]",
            Self::Image => "[IMG]",
            Self::Video => "[VID]",
            Self::Text => "[TXT]",
            Self::Archive => "[ZIP]",
            Self::Unknown => "[FILE]",
        }
    }

    fn color(self) -> Color {
        match self {
            Self::Folder => Color::Cyan,
            Self::Image => Color::Magenta,
            Self::Video => Color::LightRed,
            Self::Text => Color::Green,
            Self::Archive => Color::Yellow,
            Self::Unknown => Color::Gray,
        }
    }
}

struct Entry {
    name: String,
    is_dir: bool,
    kind: FileKind,
    size: u64,
    modified: String,
}

/// Read-only browser restricted to one configured storage directory.
struct LocalBrowser {
    root: PathBuf,
}

impl LocalBrowser {
    fn new(root: impl AsRef<Path>) -> io::Result<Self> {
        let configured_root = root.as_ref();

        // Never create the storage directory automatically.
        let metadata = fs::symlink_metadata(configured_root).map_err(|error| {
            if error.kind() == io::ErrorKind::NotFound {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Storage directory does not exist: {}",
                        configured_root.display()
                    ),
                )
            } else {
                error
            }
        })?;

        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Storage root must be a real directory, not a symbolic link",
            ));
        }

        let root = configured_root.canonicalize()?;

        Ok(Self { root })
    }

    fn checked_path(&self, relative: &Path) -> io::Result<PathBuf> {
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Path must remain inside the storage directory",
            ));
        }

        let candidate = self.root.join(relative);
        let mut current = self.root.clone();

        for component in relative.components() {
            let Component::Normal(name) = component else {
                continue;
            };

            current.push(name);

            let metadata = fs::symlink_metadata(&current)?;

            if metadata.file_type().is_symlink() {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "Symbolic links are not allowed",
                ));
            }
        }

        let canonical = candidate.canonicalize()?;

        if !canonical.starts_with(&self.root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Path is outside the storage directory",
            ));
        }

        Ok(canonical)
    }

    fn entries(&self, relative: &Path) -> io::Result<Vec<Entry>> {
        let directory = self.checked_path(relative)?;

        if !directory.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Selected path is not a directory",
            ));
        }

        let mut entries = Vec::new();

        for item in fs::read_dir(&directory)? {
            let item = item?;
            let file_type = item.file_type()?;

            // Never follow or display symbolic links.
            if file_type.is_symlink() {
                continue;
            }

            if file_type.is_dir() || file_type.is_file() {
                let name = item.file_name().to_string_lossy().into_owned();
                let metadata = item.metadata()?;
                let is_dir = file_type.is_dir();

                let modified = metadata
                    .modified()
                    .ok()
                    .and_then(|time| time.duration_since(std::time::UNIX_EPOCH).ok())
                    .map(|duration| format!("Unix timestamp: {}", duration.as_secs()))
                    .unwrap_or_else(|| "Unavailable".to_string());

                entries.push(Entry {
                    kind: FileKind::from_name(&name, is_dir),
                    name,
                    is_dir,
                    size: metadata.len(),
                    modified,
                });
            }
        }

        entries.sort_by_key(|entry| (!entry.is_dir, entry.name.to_lowercase()));

        Ok(entries)
    }

    fn read_text_file(&self, relative: &Path) -> io::Result<String> {
        let path = self.checked_path(relative)?;
        let metadata = fs::metadata(&path)?;

        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Only regular text files can be previewed",
            ));
        }

        if metadata.len() > MAX_PREVIEW_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File is larger than the 1 MiB preview limit",
            ));
        }

        fs::read_to_string(path)
    }

    /// Open a validated local media file in its OS-associated application.
    fn open_media(&self, relative: &Path) -> io::Result<()> {
        let path = self.checked_path(relative)?;
        let metadata = fs::metadata(&path)?;

        if !metadata.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Only regular files can be opened as media",
            ));
        }

        // Recheck the extension-based classification before launching.
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();

        if !matches!(
            FileKind::from_name(name, false),
            FileKind::Image | FileKind::Video
        ) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "This file type is not supported for media opening",
            ));
        }

        // Pass the path directly to the cross-platform launcher.
        // Do not construct a shell command from the filename.
        open::that(&path).map_err(|error| {
            io::Error::other(format!(
                "Could not open the file in its default application: {error}"
            ))
        })
    }
}

struct App {
    current_dir: PathBuf,
    entries: Vec<Entry>,
    selected: usize,
    message: String,
    text_preview: Option<(String, String)>,
}

impl App {
    fn new(storage: &LocalBrowser) -> io::Result<Self> {
        Ok(Self {
            current_dir: PathBuf::new(),
            entries: storage.entries(Path::new(""))?,
            selected: 0,
            message: "Welcome to InfiniteFS".into(),
            text_preview: None,
        })
    }

    fn refresh(&mut self, storage: &LocalBrowser) {
        self.text_preview = None;

        match storage.entries(&self.current_dir) {
            Ok(entries) => {
                self.entries = entries;
                self.selected = self.selected.min(self.entries.len().saturating_sub(1));

                self.message = if self.entries.is_empty() {
                    "This directory is empty.".into()
                } else {
                    "Directory loaded successfully.".into()
                };
            }
            Err(error) => {
                self.message = format!("Cannot list directory: {error}");
            }
        }
    }

    fn open_selected(&mut self, storage: &LocalBrowser) {
        let Some(entry) = self.entries.get(self.selected) else {
            self.message = "No entry selected.".into();
            return;
        };

        let name = entry.name.clone();
        let is_dir = entry.is_dir;
        let kind = entry.kind;

        self.text_preview = None;

        if is_dir {
            self.current_dir.push(&name);
            self.selected = 0;
            self.refresh(storage);
            return;
        }

        let relative_path = self.current_dir.join(&name);

        match kind {
            FileKind::Text => match storage.read_text_file(&relative_path) {
                Ok(contents) => {
                    self.text_preview = Some((name, contents));
                    self.message = "Text preview loaded.".into();
                }
                Err(error) => {
                    self.message = format!("Cannot preview text file: {error}");
                }
            },

            FileKind::Image | FileKind::Video => {
                let description = if kind == FileKind::Image {
                    "image"
                } else {
                    "video"
                };

                match storage.open_media(&relative_path) {
                    Ok(()) => {
                        self.message =
                            format!("Requested the OS to open the {description}: {name}");
                    }
                    Err(error) => {
                        self.message = format!("Cannot open media: {error}");
                    }
                }
            }

            FileKind::Archive => {
                self.message = "Archive opening is not enabled in this version.".into();
            }

            FileKind::Unknown => {
                self.message = "Unsupported file type. The file has not been opened.".into();
            }

            FileKind::Folder => {}
        }
    }

    fn go_up(&mut self, storage: &LocalBrowser) {
        if self.current_dir.pop() {
            self.selected = 0;
            self.refresh(storage);
        } else {
            self.message = "Already at the storage root.".into();
        }
    }

    fn selected_entry(&self) -> Option<&Entry> {
        self.entries.get(self.selected)
    }
}

pub fn run(root: impl AsRef<Path>) -> io::Result<()> {
    let storage = LocalBrowser::new(root)?;
    let mut app = App::new(&storage)?;

    let terminal = ratatui::init();
    let _terminal_guard = TerminalGuard;

    run_loop(terminal, &mut app, &storage)
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ratatui::restore();
    }
}

fn run_loop(
    mut terminal: DefaultTerminal,
    app: &mut App,
    storage: &LocalBrowser,
) -> io::Result<()> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            if app.text_preview.is_some() {
                match key.code {
                    KeyCode::Esc | KeyCode::Backspace => {
                        app.text_preview = None;
                        app.message = "Returned to file browser.".into();
                    }
                    KeyCode::Char('q') => return Ok(()),
                    _ => {}
                }

                continue;
            }

            match key.code {
                KeyCode::Char('q') | KeyCode::Esc => return Ok(()),

                KeyCode::Down => {
                    if !app.entries.is_empty() {
                        app.selected = (app.selected + 1) % app.entries.len();
                    }
                }

                KeyCode::Up => {
                    if !app.entries.is_empty() {
                        app.selected = if app.selected == 0 {
                            app.entries.len() - 1
                        } else {
                            app.selected - 1
                        };
                    }
                }

                KeyCode::Enter => app.open_selected(storage),
                KeyCode::Backspace => app.go_up(storage),
                _ => {}
            }
        }
    }
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GiB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MiB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KiB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} bytes")
    }
}

fn breadcrumb(path: &Path) -> String {
    if path.as_os_str().is_empty() {
        return "/".to_string();
    }

    format!(
        "/{}",
        path.components()
            .filter_map(|component| match component {
                Component::Normal(name) => Some(name.to_string_lossy().into_owned()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("/")
    )
}

fn draw(frame: &mut Frame, app: &App) {
    if let Some((filename, contents)) = &app.text_preview {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(5),
                Constraint::Length(3),
            ])
            .split(frame.area());

        let title = Paragraph::new(format!("InfiniteFS  |  Text Preview: {filename}"))
            .style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            )
            .block(Block::default().borders(Borders::ALL));

        let body = Paragraph::new(contents.as_str())
            .block(
                Block::default()
                    .borders(Borders::ALL)
                    .title("File Contents"),
            )
            .wrap(Wrap { trim: false });

        let footer = Paragraph::new("Esc/Backspace: Back to files | q: Quit")
            .style(Style::default().fg(Color::Yellow))
            .block(Block::default().borders(Borders::ALL));

        frame.render_widget(title, layout[0]);
        frame.render_widget(body, layout[1]);
        frame.render_widget(footer, layout[2]);
        return;
    }

    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header = Paragraph::new(format!(
        " INFINITEFS  |  LOCAL STORAGE  |  {}",
        breadcrumb(&app.current_dir)
    ))
    .style(
        Style::default()
            .fg(Color::Cyan)
            .add_modifier(Modifier::BOLD),
    )
    .block(Block::default().borders(Borders::ALL));

    frame.render_widget(header, vertical[0]);

    let panels = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(55), Constraint::Percentage(45)])
        .split(vertical[1]);

    let items: Vec<ListItem> = app
        .entries
        .iter()
        .map(|entry| {
            let label = if entry.is_dir {
                format!("{}  {}/", entry.kind.icon(), entry.name)
            } else {
                format!("{}  {}", entry.kind.icon(), entry.name)
            };

            ListItem::new(label).style(Style::default().fg(entry.kind.color()))
        })
        .collect();

    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(format!(" Files ({}) ", app.entries.len())),
        )
        .highlight_style(
            Style::default()
                .bg(Color::Rgb(35, 75, 120))
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    let mut state = ListState::default();

    if !app.entries.is_empty() {
        state.select(Some(app.selected));
    }

    frame.render_stateful_widget(list, panels[0], &mut state);

    let details = if let Some(entry) = app.selected_entry() {
        let size = if entry.is_dir {
            "—".to_string()
        } else {
            format_size(entry.size)
        };

        let preview_hint = match entry.kind {
            FileKind::Folder => "Press Enter to open this folder.",
            FileKind::Text => "Press Enter to preview text.",
            FileKind::Image => "Press Enter to open in the default image viewer.",
            FileKind::Video => "Press Enter to open in the default video player.",
            FileKind::Archive => "Archive opening is not enabled.",
            FileKind::Unknown => "Opening this file type is disabled.",
        };

        format!(
            "Name\n{}\n\nType\n{}\n\nSize\n{}\n\nModified\n{}\n\nAction\n{}\n\n\n{}",
            entry.name,
            entry.kind.label(),
            size,
            entry.modified,
            preview_hint,
            app.message
        )
    } else {
        format!(
            "No file selected.\n\nThis directory contains no visible files or folders.\n\n{}",
            app.message
        )
    };

    let details_panel = Paragraph::new(details).wrap(Wrap { trim: false }).block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::DarkGray))
            .title(" Metadata & Actions "),
    );

    frame.render_widget(details_panel, panels[1]);

    let footer = Paragraph::new(CONTROLS)
        .style(Style::default().fg(Color::Yellow))
        .block(Block::default().borders(Borders::ALL).title(" Keyboard "));

    frame.render_widget(footer, vertical[2]);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifies_common_file_types() {
        assert_eq!(FileKind::from_name("photo.JPG", false), FileKind::Image);
        assert_eq!(FileKind::from_name("clip.mp4", false), FileKind::Video);
        assert_eq!(FileKind::from_name("notes.txt", false), FileKind::Text);
        assert_eq!(FileKind::from_name("backup.zip", false), FileKind::Archive);
        assert_eq!(FileKind::from_name("unknown.bin", false), FileKind::Unknown);
        assert_eq!(FileKind::from_name("documents", true), FileKind::Folder);
    }

    #[test]
    fn formats_file_sizes() {
        assert_eq!(format_size(100), "100 bytes");
        assert_eq!(format_size(1024), "1.00 KiB");
        assert_eq!(format_size(1024 * 1024), "1.00 MiB");
    }

    #[test]
    fn formats_root_breadcrumb() {
        assert_eq!(breadcrumb(Path::new("")), "/");
    }

    #[test]
    fn formats_nested_breadcrumb() {
        assert_eq!(
            breadcrumb(Path::new("documents/projects")),
            "/documents/projects"
        );
    }
}
