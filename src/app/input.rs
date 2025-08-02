use crate::{
    app::{Screen, SelectableWidget},
    event::AppEvent,
};

use chrono::{DateTime, Duration, Utc};
use crossterm::event::{KeyCode, KeyEvent};
use std::{cell::Cell, rc::Rc};

/// Holds data relevant to a key press that was buffered because it did not directly map to an
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

/// The [`InputMapper`] is responsible for mapping key presses that are input by the user to
/// [`AppEvent`]s to publish on the [`EventBus`].
#[derive(Debug)]
pub struct InputMapper {
    /// Stores the widget that the user currently has selected.
    selected_widget: Rc<Cell<SelectableWidget>>,
    /// If available, contains the last key pressed that did not map to an active key binding.
    buffered_key_press: Option<BufferedKeyPress>,
}

impl InputMapper {
    /// Creates a new [`InputMapper`] with the specified dependencies.
    pub fn new(selected_widget: Rc<Cell<SelectableWidget>>) -> Self {
        Self {
            selected_widget,
            buffered_key_press: None,
        }
    }
    /// Handles key events emitted by the [`EventBus`]. Returns an [`AppEvent`] if an action was
    /// mapped for the key press.
    pub fn on_key_event(&mut self, key_event: KeyEvent) -> Option<AppEvent> {
        match key_event.code {
            KeyCode::Esc => Some(AppEvent::Quit),
            KeyCode::Tab => Some(AppEvent::SelectNextWidget),
            KeyCode::Char(c) => match c {
                '1' => Some(AppEvent::SelectScreen(Screen::ConsumeTopic)),
                '2' => Some(AppEvent::SelectScreen(Screen::NotificationHistory)),
                _ => {
                    let event = self.key_press_on_widget(c);

                    if event.is_some() {
                        self.buffered_key_press = None;
                    } else {
                        self.buffered_key_press = Some(BufferedKeyPress::new(c));
                    }

                    event
                }
            },
            _ => None,
        }
    }
    /// Attempts to map a key press to an action based on the currently selected widget.
    fn key_press_on_widget(&self, key: char) -> Option<AppEvent> {
        let widget = self.selected_widget.get();

        match widget {
            SelectableWidget::RecordList => match key {
                'e' => Some(AppEvent::ExportSelectedRecord),
                'p' => Some(AppEvent::PauseProcessing),
                'r' => Some(AppEvent::ResumeProcessing),
                'g' if self.is_key_buffered('g') => Some(AppEvent::SelectFirstRecord),
                'j' => Some(AppEvent::SelectNextRecord),
                'k' => Some(AppEvent::SelectPrevRecord),
                'G' => Some(AppEvent::SelectLastRecord),
                _ => None,
            },
            // TODO: cleanup duplication of e,p and r
            SelectableWidget::RecordValue => match key {
                'e' => Some(AppEvent::ExportSelectedRecord),
                'p' => Some(AppEvent::PauseProcessing),
                'r' => Some(AppEvent::ResumeProcessing),
                'g' if self.is_key_buffered('g') => Some(AppEvent::ScrollRecordValueTop),
                'j' => Some(AppEvent::ScrollRecordValueDown),
                'k' => Some(AppEvent::ScrollRecordValueUp),
                _ => None,
            },
            SelectableWidget::NotificationHistoryList => match key {
                'g' if self.is_key_buffered('g') => Some(AppEvent::ScrollNotificationHistoryTop),
                'j' => Some(AppEvent::ScrollNotificationHistoryDown),
                'k' => Some(AppEvent::ScrollNotificationHistoryUp),
                'G' => Some(AppEvent::ScrollNotificationHistoryBottom),
                _ => None,
            },
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
