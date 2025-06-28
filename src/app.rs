use crate::event::{AppEvent, Event, EventBus};

use ratatui::{
    crossterm::event::{KeyCode, KeyEvent, KeyModifiers},
    DefaultTerminal,
};

#[derive(Debug)]
pub struct State {
    /// Flag indicating the application is running.
    pub running: bool,
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
        Self { running: true }
    }
}

#[derive(Debug)]
pub struct App {
    pub state: State,
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

        while self.state.running {
            terminal.draw(|frame| frame.render_widget(&self, frame.area()))?;

            match self.event_bus.next().await? {
                Event::Tick => self.tick(),
                Event::Crossterm(event) => match event {
                    crossterm::event::Event::Key(key_event) => self.handle_key_events(key_event)?,
                    _ => {}
                },
                Event::App(app_event) => match app_event {
                    AppEvent::Quit => self.quit(),
                },
            }
        }

        Ok(())
    }
    /// Handles the key events and updates the state of [`App`].
    pub fn handle_key_events(&mut self, key_event: KeyEvent) -> anyhow::Result<()> {
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
    ///
    /// The tick event is where you can update the state of your application with any logic that
    /// needs to be updated at a fixed frame rate. E.g. polling a server, updating an animation.
    pub fn tick(&self) {}
    /// Set running to false to quit the application.
    pub fn quit(&mut self) {
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
