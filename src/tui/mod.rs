use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use ratatui::{
    DefaultTerminal, Frame,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    fs, io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

struct DemoStorage {
    root: PathBuf,
}

impl DemoStorage {
    fn new() -> io::Result<Self> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        let temporary_root = std::env::temp_dir().join(format!("infinitefs-tui-demo-{unique}"));

        fs::create_dir_all(temporary_root.join("Documents"))?;
        fs::create_dir_all(temporary_root.join("Projects"))?;

        fs::write(
            temporary_root.join("welcome.txt"),
            "Welcome to InfiniteFS!\nThis is temporary demo data.",
        )?;

        fs::write(
            temporary_root.join("Documents").join("notes.txt"),
            "Your notes will appear here.",
        )?;

        fs::write(
            temporary_root.join("Projects").join("readme.txt"),
            "Example project folder.",
        )?;

        let root = temporary_root.canonicalize()?;

        Ok(Self { root })
    }

    fn entries(&self, relative: &Path) -> io::Result<Vec<Entry>> {
        // The TUI only browses paths relative to its isolated demo root.
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Invalid directory",
            ));
        }

        let directory = self.root.join(relative);
        let canonical = directory.canonicalize()?;

        if !canonical.starts_with(&self.root) || !canonical.is_dir() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Invalid directory",
            ));
        }

        let mut entries = Vec::new();

        for item in fs::read_dir(&canonical)? {
            let item = item?;
            let file_type = item.file_type()?;

            // Do not follow or display symbolic links.
            if file_type.is_symlink() {
                continue;
            }

            if file_type.is_dir() || file_type.is_file() {
                entries.push(Entry {
                    name: item.file_name().to_string_lossy().into_owned(),
                    is_dir: file_type.is_dir(),
                });
            }
        }

        // Display directories first, followed by files, alphabetically.
        entries.sort_by_key(|entry| (!entry.is_dir, entry.name.to_lowercase()));

        Ok(entries)
    }

    fn read_text_file(&self, relative: &Path) -> io::Result<String> {
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Invalid file path",
            ));
        }

        let path = self.root.join(relative);
        let metadata = fs::symlink_metadata(&path)?;

        if !metadata.file_type().is_file() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Only regular text files can be previewed",
            ));
        }

        let canonical = path.canonicalize()?;

        if !canonical.starts_with(&self.root) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "File is outside the demo directory",
            ));
        }

        // Limit preview size to avoid loading large files into memory.
        const MAX_PREVIEW_BYTES: u64 = 1024 * 1024;

        if fs::metadata(&canonical)?.len() > MAX_PREVIEW_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "File is larger than the 1 MiB preview limit",
            ));
        }

        fs::read_to_string(canonical)
    }
}

impl Drop for DemoStorage {
    fn drop(&mut self) {
        // Only remove this run's temporary demo directory.
        let _ = fs::remove_dir_all(&self.root);
    }
}

struct Entry {
    name: String,
    is_dir: bool,
}

struct App {
    current_dir: PathBuf,
    entries: Vec<Entry>,
    selected: usize,
    message: String,
    preview: Option<(String, String)>,
}

impl App {
    fn new(storage: &DemoStorage) -> io::Result<Self> {
        Ok(Self {
            current_dir: PathBuf::new(),
            entries: storage.entries(Path::new(""))?,
            selected: 0,
            message: "Up/Down: Navigate | Enter: Open | Backspace: Up | q: Quit".into(),
            preview: None,
        })
    }

    fn refresh(&mut self, storage: &DemoStorage) {
        match storage.entries(&self.current_dir) {
            Ok(entries) => {
                self.entries = entries;
                self.selected = self.selected.min(self.entries.len().saturating_sub(1));
            }
            Err(error) => {
                self.message = format!("Could not list directory: {error}");
            }
        }
    }

    fn open_selected(&mut self, storage: &DemoStorage) {
        let Some(entry) = self.entries.get(self.selected) else {
            return;
        };

        let name = entry.name.clone();
        let is_dir = entry.is_dir;

        if is_dir {
            self.current_dir.push(&name);
            self.selected = 0;
            self.refresh(storage);
            return;
        }

        let relative_path = self.current_dir.join(&name);

        match storage.read_text_file(&relative_path) {
            Ok(contents) => {
                self.preview = Some((name, contents));
            }
            Err(error) => {
                self.message = format!("Could not preview file: {error}");
            }
        }
    }

    fn go_up(&mut self, storage: &DemoStorage) {
        if self.current_dir.pop() {
            self.selected = 0;
            self.refresh(storage);
        }
    }
}

pub fn run() -> io::Result<()> {
    let storage = DemoStorage::new()?;
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

fn run_loop(mut terminal: DefaultTerminal, app: &mut App, storage: &DemoStorage) -> io::Result<()> {
    loop {
        terminal.draw(|frame| draw(frame, app))?;

        if let Event::Key(key) = event::read()? {
            if key.kind != KeyEventKind::Press {
                continue;
            }

            // While previewing, keep navigation keys inside the preview.
            if app.preview.is_some() {
                match key.code {
                    KeyCode::Esc | KeyCode::Backspace => {
                        app.preview = None;
                        app.message =
                            "Up/Down: Navigate | Enter: Open | Backspace: Up | q: Quit".into();
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

fn draw(frame: &mut Frame, app: &App) {
    if let Some((filename, contents)) = &app.preview {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(5),
                Constraint::Length(3),
            ])
            .split(frame.area());

        let title = Paragraph::new(format!("Preview: {filename}"))
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

        let footer = Paragraph::new("Esc/Backspace: Return to files | q: Quit")
            .block(Block::default().borders(Borders::ALL));

        frame.render_widget(title, layout[0]);
        frame.render_widget(body, layout[1]);
        frame.render_widget(footer, layout[2]);

        return;
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let path = if app.current_dir.as_os_str().is_empty() {
        "/".to_string()
    } else {
        format!(
            "/{}",
            app.current_dir.display().to_string().replace('\\', "/")
        )
    };

    let title = Paragraph::new(format!("InfiniteFS | {path}"))
        .style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .block(Block::default().borders(Borders::ALL).title("File Browser"));

    frame.render_widget(title, layout[0]);

    let items: Vec<ListItem> = app
        .entries
        .iter()
        .map(|entry| {
            let label = if entry.is_dir {
                format!("[DIR]  {}/", entry.name)
            } else {
                format!("       {}", entry.name)
            };

            ListItem::new(label)
        })
        .collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::ALL).title("Entries"))
        .highlight_style(
            Style::default()
                .bg(Color::Blue)
                .fg(Color::White)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("> ");

    let mut state = ListState::default();

    if !app.entries.is_empty() {
        state.select(Some(app.selected));
    }

    frame.render_stateful_widget(list, layout[1], &mut state);

    let footer = Paragraph::new(app.message.as_str())
        .block(Block::default().borders(Borders::ALL).title("Controls"));

    frame.render_widget(footer, layout[2]);
}
