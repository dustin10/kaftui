use crate::{
    app::{Screen, SelectableWidget},
    event::{AppEvent, EventBus},
};

use chrono::{DateTime, Duration, Utc};
use crossterm::event::{KeyCode, KeyEvent};
use std::{cell::Cell, rc::Rc, sync::Arc};

/// Holds data relevant to a key press that is was buffered because it did not directly map to an
/// action. This is used for a simple implementation of vim-style key bindings, e.g. `gg` is bound
/// to selecting the first record in the list.
#[derive(Debug)]
struct BufferedKeyPress {
    /// Last key that was pressed that did not map to an action.
    key: char,
    /// Time that the buffered key press will expire.
    ttl: DateTime<Utc>,
}

impl BufferedKeyPress {
    /// Creates a new [`BufferedKeyPress`] with the key that was pressed by the user.
    fn new(key: char) -> Self {
        Self {
            key,
            ttl: Utc::now() + Duration::seconds(1),
        }
    }
    /// Determines if the key press matches the specified character. False will always be returned
    /// if the key press has expired.
    fn is(&self, key: char) -> bool {
        !self.is_expired() && self.key == key
    }
    /// Determines if the key press has expired based on the TTL that was set when it was initially
    /// buffered.
    fn is_expired(&self) -> bool {
        self.ttl < Utc::now()
    }
}

/// The [`InputDispatcher`] is responsible for mapping key presses that are input by the user to
/// actions published on the [`EventBus`].
#[derive(Debug)]
pub struct InputDispatcher {
    /// Emits events to be handled by the application.
    event_bus: Arc<EventBus>,
    /// Stores the widget that the user currently has selected.
    selected_widget: Rc<Cell<SelectableWidget>>,
    /// If available, contains the last key pressed that did not map to an active key binding.
    buffered_key_press: Option<BufferedKeyPress>,
}

impl InputDispatcher {
    /// Creates a new [`InputDispatcher`] with the specified dependencies.
    pub fn new(event_bus: Arc<EventBus>, selected_widget: Rc<Cell<SelectableWidget>>) -> Self {
        Self {
            event_bus,
            selected_widget,
            buffered_key_press: None,
        }
    }
    /// Handles key events emitted by the [`EventBus`].
    pub async fn on_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Esc => self.event_bus.send(AppEvent::Quit).await,
            KeyCode::Tab => self.event_bus.send(AppEvent::SelectNextWidget).await,
            KeyCode::Char(c) => match c {
                '1' => {
                    self.event_bus
                        .send(AppEvent::SelectScreen(Screen::ConsumeTopic))
                        .await
                }
                '2' => {
                    self.event_bus
                        .send(AppEvent::SelectScreen(Screen::NotificationHistory))
                        .await
                }
                _ => {
                    if self.key_press_on_widget(c).await {
                        self.buffered_key_press = None;
                    } else {
                        self.buffered_key_press = Some(BufferedKeyPress::new(c));
                    }
                }
            },
            _ => {}
        }
    }
    /// Attempts to map a key press to an action based on the currently active widget. Returns true
    /// if an action was mapped for the key press, false otherwise.
    async fn key_press_on_widget(&self, key: char) -> bool {
        let widget = self.selected_widget.get();

        // TODO: something better than returning bool here?
        match widget {
            SelectableWidget::RecordList => match key {
                'e' => {
                    self.event_bus.send(AppEvent::ExportSelectedRecord).await;
                    true
                }
                'p' => {
                    self.event_bus.send(AppEvent::PauseProcessing).await;
                    true
                }
                'r' => {
                    self.event_bus.send(AppEvent::ResumeProcessing).await;
                    true
                }
                'g' if self.is_key_buffered('g') => {
                    self.event_bus.send(AppEvent::SelectFirstRecord).await;
                    true
                }
                'j' => {
                    self.event_bus.send(AppEvent::SelectNextRecord).await;
                    true
                }
                'k' => {
                    self.event_bus.send(AppEvent::SelectPrevRecord).await;
                    true
                }
                'G' => {
                    self.event_bus.send(AppEvent::SelectLastRecord).await;
                    true
                }
                _ => false,
            },
            // TODO: add scroll to top
            // TODO: cleanup duplication of e,p and r
            SelectableWidget::RecordValue => match key {
                'e' => {
                    self.event_bus.send(AppEvent::ExportSelectedRecord).await;
                    true
                }
                'p' => {
                    self.event_bus.send(AppEvent::PauseProcessing).await;
                    true
                }
                'r' => {
                    self.event_bus.send(AppEvent::ResumeProcessing).await;
                    true
                }
                'j' => {
                    self.event_bus.send(AppEvent::ScrollRecordValueDown).await;
                    true
                }
                'k' => {
                    self.event_bus.send(AppEvent::ScrollRecordValueUp).await;
                    true
                }
                _ => false,
            },
            // TODO: add key bindings for notification history list
            SelectableWidget::NotificationHistoryList => false,
        }
    }
    /// Determines if the last key press that was buffered matches the given character.
    fn is_key_buffered(&self, key: char) -> bool {
        self.buffered_key_press
            .as_ref()
            .filter(|v| v.is(key))
            .is_some()
    }
}
