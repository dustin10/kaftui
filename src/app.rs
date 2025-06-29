use crate::event::{AppEvent, Event, EventBus};

use crossterm::event::MouseEvent;
use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    DefaultTerminal,
};
use std::collections::HashMap;

#[derive(Clone, Debug, Default)]
pub struct Record {
    pub headers: HashMap<String, String>,
    pub value: String,
}

#[derive(Debug)]
pub struct State {
    /// Flag indicating the application is running.
    pub running: bool,
    pub record: Option<Record>,
}

impl State {
    /// Creates a new default [`State`].
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for State {
    /// Creates a new instance of [`State`] initialized to the default state.
    fn default() -> Self {
        Self {
            running: true,
            record: None,
        }
    }
}

#[derive(Debug)]
pub struct App {
    /// Contains the current state of the application.
    pub state: State,
    /// Emits events to be handled by the application.
    pub event_bus: EventBus,
}

impl App {
    /// Creates a new default [`App`].
    pub fn new() -> Self {
        Self::default()
    }
    /// Run the main loop of the application.
    pub async fn run(mut self, mut terminal: DefaultTerminal) -> anyhow::Result<()> {
        self.event_bus.start();

        let mut headers = HashMap::new();
        headers.insert(String::from("foo"), String::from("bar"));
        headers.insert(String::from("biz"), String::from("baz"));

        let record = Record {
            headers,
            value: String::from("{\n    \"foo\":\"bar\",\n    \"biz\":\"baz\"\n}"),
        };

        self.state.record = Some(record);

        while self.state.running {
            terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;

            match self.event_bus.next().await? {
                Event::Tick => self.tick(),
                Event::Crossterm(event) => match event {
                    crossterm::event::Event::Mouse(mouse_event) => {
                        self.on_mouse_event(mouse_event)?
                    }
                    crossterm::event::Event::Key(key_event) => self.on_key_event(key_event)?,
                    _ => {}
                },
                Event::App(app_event) => match app_event {
                    AppEvent::Quit => self.quit(),
                },
            }
        }

        Ok(())
    }
    /// Handles mouse events emitted by the [`EventBus`].
    fn on_mouse_event(&mut self, _mouse_event: MouseEvent) -> anyhow::Result<()> {
        Ok(())
    }
    /// Handles key events emitted by the [`EventBus`].
    fn on_key_event(&mut self, key_event: KeyEvent) -> anyhow::Result<()> {
        match key_event.code {
            KeyCode::Esc | KeyCode::Char('q') => self.event_bus.send(AppEvent::Quit),
            KeyCode::Char('c' | 'C') if key_event.modifiers == KeyModifiers::CONTROL => {
                self.event_bus.send(AppEvent::Quit)
            }
            _ => {}
        }

        Ok(())
    }
    /// Handles the tick event of the terminal.
    fn tick(&self) {}
    /// Quits the application.
    fn quit(&mut self) {
        self.state.running = false;
    }
}

impl Default for App {
    /// Creates a new instance of [`App`] that is intialized to the default state.
    fn default() -> Self {
        Self {
            state: State::default(),
            event_bus: EventBus::default(),
        }
    }
}
