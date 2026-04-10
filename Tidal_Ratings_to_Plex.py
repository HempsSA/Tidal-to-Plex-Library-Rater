import sys
import os
import json
import time
import webbrowser
from datetime import datetime
from typing import Dict, List, Optional
import requests
from plexapi.server import PlexServer
from plexapi.playlist import Playlist
from plexapi.audio import Album, Artist
import tidalapi
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLabel, QTextEdit, QProgressBar, QLineEdit,
    QComboBox, QCheckBox, QSpinBox, QGroupBox, QGridLayout,
    QMessageBox, QTabWidget, QTableWidget, QTableWidgetItem,
    QHeaderView, QSplitter, QFileDialog, QDialog, 
    QListWidget, QListWidgetItem, QAbstractItemView, QRadioButton,
    QButtonGroup, QTreeWidget, QTreeWidgetItem, QMenu
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal, QUrl, QTimer
from PyQt6.QtGui import QTextCursor, QColor, QAction

QVBoxLayoutDialog = QVBoxLayout


class ClearRatingsThread(QThread):
    """Thread for clearing Plex ratings"""
    
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(int)
    
    def __init__(self, plex_server, selected_items=None, clear_all=False):
        super().__init__()
        self.plex_server = plex_server
        self.selected_items = selected_items
        self.clear_all = clear_all
        self.is_running = True
        
    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_signal.emit(f"[{timestamp}] {message}")
        
    def run(self):
        try:
            tracks_to_clear = []
            
            if self.selected_items:
                self.log(f"Collecting tracks from {len(self.selected_items)} selected items...")
                for item_data in self.selected_items:
                    if not self.is_running:
                        break
                    try:
                        if item_data['type'] == 'playlist':
                            playlist = item_data['object']
                            self.log(f"Loading playlist: {playlist.title}")
                            tracks_to_clear.extend(playlist.items())
                        elif item_data['type'] == 'album':
                            album = item_data['object']
                            self.log(f"Loading album: {album.title}")
                            tracks_to_clear.extend(album.tracks())
                        elif item_data['type'] == 'artist':
                            artist = item_data['object']
                            self.log(f"Loading artist: {artist.title}")
                            tracks_to_clear.extend(artist.tracks())
                    except Exception as e:
                        self.log(f"Error loading item: {str(e)}")
            elif self.clear_all:
                self.log("Collecting all tracks from music library...")
                music_sections = [s for s in self.plex_server.library.sections() if s.type == 'artist']
                if not music_sections:
                    self.log("No music library found!")
                    return
                
                music_section = music_sections[0]
                self.log(f"Found music library: {music_section.title}")
                
                try:
                    for album in music_section.albums():
                        if not self.is_running:
                            break
                        try:
                            tracks_to_clear.extend(album.tracks())
                        except:
                            pass
                except Exception as e:
                    self.log(f"Error fetching albums: {str(e)}")
                    return
            
            total_tracks = len(tracks_to_clear)
            self.log(f"Found {total_tracks} tracks to clear ratings from")
            self.status_signal.emit(f"Clearing ratings from {total_tracks} tracks...")
            
            cleared_count = 0
            for idx, track in enumerate(tracks_to_clear):
                if not self.is_running:
                    break
                
                self.progress_signal.emit(idx + 1, total_tracks)
                
                try:
                    track.rate(None)
                    cleared_count += 1
                    if idx % 50 == 0:
                        self.log(f"Cleared ratings from {idx + 1}/{total_tracks} tracks")
                except:
                    pass
                
                if idx % 10 == 0:
                    time.sleep(0.01)
            
            self.log(f"Successfully cleared ratings from {cleared_count} tracks")
            self.status_signal.emit(f"Cleared {cleared_count} ratings")
            self.finished_signal.emit(cleared_count)
            
        except Exception as e:
            self.log(f"Error clearing ratings: {str(e)}")
            self.status_signal.emit(f"Error: {str(e)}")
    
    def stop(self):
        self.is_running = False


class TidalLoginThread(QThread):
    """Thread for handling Tidal OAuth login"""
    
    login_success = pyqtSignal(object)
    login_error = pyqtSignal(str)
    log_signal = pyqtSignal(str)
    auth_url_signal = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        self.session = None
        self.login = None
        self.future = None
        
    def run(self):
        try:
            self.log_signal.emit("Creating Tidal session...")
            self.session = tidalapi.Session()
            self.log_signal.emit("Starting OAuth flow...")
            
            self.login, self.future = self.session.login_oauth()
            auth_url = self.login.verification_uri_complete
            self.log_signal.emit("Authentication required")
            self.auth_url_signal.emit(auth_url)
            self.log_signal.emit("Waiting for authentication (this may take up to 2 minutes)...")
            
            for i in range(120):
                if self.future.done():
                    break
                time.sleep(1)
            
            if self.future.done():
                self.future.result()
                if self.session.check_login():
                    self.log_signal.emit("Login successful!")
                    self.login_success.emit(self.session)
                else:
                    self.login_error.emit("Login verification failed")
            else:
                self.login_error.emit("Login timeout - please try again")
                
        except Exception as e:
            self.log_signal.emit(f"Login error: {str(e)}")
            self.login_error.emit(str(e))
    
    def cancel(self):
        if self.future and not self.future.done():
            self.future.cancel()


class LibraryLoaderThread(QThread):
    """Thread for loading library items in background"""
    
    items_loaded = pyqtSignal(list, str)
    error_occurred = pyqtSignal(str)
    progress_update = pyqtSignal(str)
    
    def __init__(self, plex_server, selection_type):
        super().__init__()
        self.plex = plex_server
        self.selection_type = selection_type
        
    def run(self):
        try:
            items = []
            music_section = None
            for section in self.plex.library.sections():
                if section.type == 'artist':
                    music_section = section
                    break
            
            if not music_section:
                self.error_occurred.emit("No music library found!")
                return
            
            if self.selection_type == "playlist":
                self.progress_update.emit("Loading playlists...")
                try:
                    playlists = self.plex.playlists()
                    audio_playlists = [p for p in playlists if hasattr(p, 'playlistType') and p.playlistType == 'audio']
                    for idx, playlist in enumerate(audio_playlists):
                        self.progress_update.emit(f"Loading playlist {idx + 1}/{len(audio_playlists)}: {playlist.title}")
                        items.append({
                            'name': playlist.title,
                            'type': 'playlist',
                            'object': playlist,
                            'count': '?'
                        })
                except Exception as e:
                    self.error_occurred.emit(f"Could not load playlists: {str(e)}")
                    
            elif self.selection_type == "album":
                self.progress_update.emit("Loading albums...")
                try:
                    albums = list(music_section.albums())
                    for idx, album in enumerate(albums[:500]):
                        if idx % 50 == 0:
                            self.progress_update.emit(f"Loading album {idx + 1}/{min(len(albums), 500)}: {album.title}")
                        items.append({
                            'name': album.title,
                            'type': 'album',
                            'object': album,
                            'count': '?'
                        })
                except Exception as e:
                    self.error_occurred.emit(f"Could not load albums: {str(e)}")
                    
            elif self.selection_type == "artist":
                self.progress_update.emit("Loading artists...")
                try:
                    artists = list(music_section.all())
                    for idx, artist in enumerate(artists[:500]):
                        if idx % 50 == 0:
                            self.progress_update.emit(f"Loading artist {idx + 1}/{min(len(artists), 500)}: {artist.title}")
                        items.append({
                            'name': artist.title,
                            'type': 'artist',
                            'object': artist,
                            'count': '?'
                        })
                except Exception as e:
                    self.error_occurred.emit(f"Could not load artists: {str(e)}")
            
            self.progress_update.emit(f"Loaded {len(items)} items")
            self.items_loaded.emit(items, self.selection_type)
            
        except Exception as e:
            self.error_occurred.emit(f"Error loading items: {str(e)}")


class TidalPlexMatcher(QThread):
    """Worker thread for matching Plex songs with Tidal"""
    
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    match_found_signal = pyqtSignal(dict)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(list)
    
    def __init__(self, plex_url, plex_token, tidal_session, options, selected_items=None):
        super().__init__()
        self.plex_url = plex_url
        self.plex_token = plex_token
        self.tidal_session = tidal_session
        self.options = options
        self.selected_items = selected_items
        self.is_running = True
        self.matches = []
        
    def log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_signal.emit(f"[{timestamp}] {message}")
        
    def run(self):
        try:
            self.log("Connecting to Plex server...")
            self.status_signal.emit("Connecting to Plex...")
            plex = PlexServer(self.plex_url, self.plex_token)
            
            all_tracks = []
            
            if self.selected_items:
                self.log(f"Processing {len(self.selected_items)} selected items...")
                for item_data in self.selected_items:
                    if not self.is_running:
                        break
                    try:
                        if item_data['type'] == 'playlist':
                            playlist = item_data['object']
                            self.log(f"Loading playlist: {playlist.title}")
                            all_tracks.extend(playlist.items())
                        elif item_data['type'] == 'album':
                            album = item_data['object']
                            self.log(f"Loading album: {album.title}")
                            all_tracks.extend(album.tracks())
                        elif item_data['type'] == 'artist':
                            artist = item_data['object']
                            self.log(f"Loading artist: {artist.title}")
                            all_tracks.extend(artist.tracks())
                    except Exception as e:
                        self.log(f"Error loading item: {str(e)}", "DEBUG")
            else:
                music_sections = [s for s in plex.library.sections() if s.type == 'artist']
                if not music_sections:
                    self.log("No music library found!", "ERROR")
                    return
                
                music_section = music_sections[0]
                self.log(f"Found music library: {music_section.title}")
                
                if self.options.get('limit_artists'):
                    artists = list(music_section.all())[:self.options['artist_limit']]
                    for artist in artists:
                        try:
                            artist_tracks = list(artist.tracks())
                            all_tracks.extend(artist_tracks)
                            self.log(f"Loaded {len(artist_tracks)} tracks from {artist.title}")
                        except Exception as e:
                            self.log(f"Error loading tracks for {artist.title}: {str(e)}", "DEBUG")
                else:
                    try:
                        for album in music_section.albums():
                            try:
                                all_tracks.extend(album.tracks())
                            except:
                                pass
                    except Exception as e:
                        self.log(f"Error fetching albums: {str(e)}", "ERROR")
                        return
            
            total_tracks = len(all_tracks)
            self.log(f"Found {total_tracks} tracks to process")
            
            matches = []
            for idx, track in enumerate(all_tracks):
                if not self.is_running:
                    break
                    
                self.progress_signal.emit(idx + 1, total_tracks)
                
                try:
                    artist_name = track.artist().title if track.artist() else "Unknown Artist"
                except:
                    continue
                
                self.status_signal.emit(f"Processing: {track.title} by {artist_name}")
                
                if idx % 10 == 0:
                    self.log(f"Processing track {idx + 1}/{total_tracks}: {track.title}")
                
                tidal_match = self.search_tidal_track(track)
                
                if tidal_match:
                    popularity = getattr(tidal_match, 'popularity', 0)
                    match_info = {
                        'plex_track': track,
                        'tidal_track': tidal_match,
                        'popularity': popularity if popularity else 0,
                        'match_score': self.calculate_match_score(track, tidal_match)
                    }
                    matches.append(match_info)
                    self.match_found_signal.emit(match_info)
                    
                    if self.options.get('update_ratings'):
                        try:
                            rating = min(10, max(0, popularity / 10))
                            track.rate(rating)
                        except:
                            pass
                    
                    time.sleep(0.1)
            
            matches.sort(key=lambda x: x['popularity'], reverse=True)
            
            self.log(f"Completed! Found {len(matches)} matches")
            self.status_signal.emit(f"Complete - {len(matches)} matches found")
            self.finished_signal.emit(matches)
            
        except Exception as e:
            self.log(f"Error: {str(e)}", "ERROR")
            self.status_signal.emit(f"Error: {str(e)}")
    
    def search_tidal_track(self, plex_track):
        try:
            artist_name = plex_track.artist().title if plex_track.artist() else ""
            track_title = plex_track.title
            query = f"{track_title} {artist_name}"
            search_results = self.tidal_session.search(query, models=[tidalapi.Track])
            
            if not search_results or 'tracks' not in search_results:
                return None
            
            tracks = search_results['tracks']
            if not tracks:
                return None
            
            best_match = None
            best_score = 0
            
            for tidal_track in tracks[:5]:
                score = self.calculate_match_score(plex_track, tidal_track)
                if score > best_score and score >= self.options['match_threshold']:
                    best_score = score
                    best_match = tidal_track
            
            if best_match and hasattr(best_match, 'id'):
                try:
                    full_track = self.tidal_session.track(best_match.id)
                    if full_track:
                        return full_track
                except:
                    pass
            
            return best_match
                
        except:
            pass
            
        return None
    
    def calculate_match_score(self, plex_track, tidal_track):
        score = 0
        try:
            plex_title = plex_track.title.lower().strip()
            tidal_title = tidal_track.name.lower().strip()
            
            if plex_title == tidal_title:
                score += 50
            elif plex_title in tidal_title or tidal_title in plex_title:
                score += 30
            
            if plex_track.artist():
                plex_artist = plex_track.artist().title.lower().strip()
                tidal_artist = tidal_track.artist.name.lower().strip()
                
                if plex_artist == tidal_artist:
                    score += 50
                elif plex_artist in tidal_artist or tidal_artist in plex_artist:
                    score += 30
            
            if hasattr(plex_track, 'duration') and hasattr(tidal_track, 'duration'):
                plex_duration = plex_track.duration
                tidal_duration = tidal_track.duration * 1000 if tidal_track.duration < 1000 else tidal_track.duration
                duration_diff = abs(plex_duration - tidal_duration)
                if duration_diff < 5000:
                    score += 20
                elif duration_diff < 10000:
                    score += 10
        except:
            pass
        return score
    
    def stop(self):
        self.is_running = False


class ClearRatingsDialog(QDialog):
    """Dialog for confirming clear ratings operation"""
    
    def __init__(self, has_selection, parent=None):
        super().__init__(parent)
        self.has_selection = has_selection
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Clear Ratings Confirmation")
        self.setMinimumWidth(500)
        
        layout = QVBoxLayoutDialog()
        
        warning_label = QLabel("⚠️ WARNING: This action cannot be undone!")
        warning_label.setStyleSheet("color: #ff6b6b; font-size: 14px; font-weight: bold;")
        layout.addWidget(warning_label)
        
        scope_group = QGroupBox("Clear Scope")
        scope_layout = QVBoxLayout()
        
        self.scope_button_group = QButtonGroup()
        
        if self.has_selection:
            self.selected_radio = QRadioButton("Clear ratings from SELECTED items only")
            self.selected_radio.setChecked(True)
            self.scope_button_group.addButton(self.selected_radio)
            scope_layout.addWidget(self.selected_radio)
        
        self.all_radio = QRadioButton("Clear ratings from ENTIRE music library")
        if not self.has_selection:
            self.all_radio.setChecked(True)
        self.scope_button_group.addButton(self.all_radio)
        scope_layout.addWidget(self.all_radio)
        
        scope_group.setLayout(scope_layout)
        layout.addWidget(scope_group)
        
        if not self.has_selection:
            warning_text = QLabel(
                "You are about to clear ratings from ALL tracks in your music library.\n"
                "This could affect thousands of tracks and cannot be undone."
            )
        else:
            warning_text = QLabel(
                "You are about to clear ratings from your selected items.\n"
                "This action cannot be undone."
            )
        warning_text.setWordWrap(True)
        warning_text.setStyleSheet("color: #ffa500; padding: 10px;")
        layout.addWidget(warning_text)
        
        self.confirm_check = QCheckBox("I understand that this action cannot be undone")
        layout.addWidget(self.confirm_check)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        self.clear_btn = QPushButton("Clear Ratings")
        self.clear_btn.setStyleSheet("background-color: #ff6b6b;")
        self.clear_btn.setEnabled(False)
        self.clear_btn.clicked.connect(self.accept)
        button_layout.addWidget(self.clear_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        
        self.confirm_check.toggled.connect(self.clear_btn.setEnabled)
        
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QGroupBox { color: #ffffff; border: 2px solid #555555; border-radius: 5px; margin-top: 10px; font-weight: bold; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }
            QRadioButton { color: #ffffff; padding: 5px; }
            QCheckBox { color: #ffffff; padding: 10px; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px 16px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
            QPushButton:disabled { background-color: #2a2a2a; color: #888888; }
        """)
    
    def get_scope(self):
        if self.has_selection and hasattr(self, 'selected_radio'):
            if self.selected_radio.isChecked():
                return "selected"
        return "all"


class AuthDialog(QDialog):
    """Dialog to show authentication URL"""
    
    def __init__(self, auth_url, parent=None):
        super().__init__(parent)
        self.auth_url = auth_url
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Tidal Authentication")
        self.setMinimumWidth(600)
        self.setMinimumHeight(200)
        
        layout = QVBoxLayoutDialog()
        
        instructions = QLabel(
            "Please open the following URL in your browser to authenticate with Tidal:\n\n"
            "After authenticating, return to this window and click 'Continue'."
        )
        instructions.setWordWrap(True)
        instructions.setStyleSheet("color: white; font-size: 12px;")
        layout.addWidget(instructions)
        
        url_label = QLabel(f'<a href="{self.auth_url}">{self.auth_url}</a>')
        url_label.setOpenExternalLinks(True)
        url_label.setStyleSheet("color: #00a8ff; font-size: 14px; padding: 10px;")
        url_label.setTextInteractionFlags(Qt.TextInteractionFlag.TextSelectableByMouse)
        layout.addWidget(url_label)
        
        copy_btn = QPushButton("Copy URL to Clipboard")
        copy_btn.clicked.connect(self.copy_url)
        layout.addWidget(copy_btn)
        
        open_btn = QPushButton("Open in Browser")
        open_btn.clicked.connect(self.open_browser)
        layout.addWidget(open_btn)
        
        continue_btn = QPushButton("I've Authenticated - Continue")
        continue_btn.clicked.connect(self.accept)
        continue_btn.setStyleSheet("background-color: #00a8ff;")
        layout.addWidget(continue_btn)
        
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
        """)
    
    def copy_url(self):
        QApplication.clipboard().setText(self.auth_url)
        QMessageBox.information(self, "Copied", "URL copied to clipboard!")
    
    def open_browser(self):
        try:
            webbrowser.open(self.auth_url)
            QMessageBox.information(self, "Browser Opened", 
                "Browser should have opened. If not, please copy and paste the URL manually.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Could not open browser: {str(e)}\nPlease copy the URL manually.")


class LibrarySelectorDialog(QDialog):
    """Dialog for selecting playlists, albums, or artists"""
    
    def __init__(self, plex_server, parent=None):
        super().__init__(parent)
        self.plex = plex_server
        self.selected_items = []
        self.selection_type = "playlist"
        self.all_items = []
        self.loader_thread = None
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Select Library Items")
        self.setMinimumSize(800, 600)
        
        layout = QVBoxLayoutDialog()
        
        type_group = QGroupBox("Selection Type")
        type_layout = QHBoxLayout()
        
        self.type_button_group = QButtonGroup()
        
        self.playlist_radio = QRadioButton("Playlists")
        self.playlist_radio.setChecked(True)
        self.playlist_radio.toggled.connect(lambda: self.on_type_changed("playlist"))
        self.type_button_group.addButton(self.playlist_radio)
        
        self.album_radio = QRadioButton("Albums")
        self.album_radio.toggled.connect(lambda: self.on_type_changed("album"))
        self.type_button_group.addButton(self.album_radio)
        
        self.artist_radio = QRadioButton("Artists")
        self.artist_radio.toggled.connect(lambda: self.on_type_changed("artist"))
        self.type_button_group.addButton(self.artist_radio)
        
        type_layout.addWidget(self.playlist_radio)
        type_layout.addWidget(self.album_radio)
        type_layout.addWidget(self.artist_radio)
        type_layout.addStretch()
        type_group.setLayout(type_layout)
        layout.addWidget(type_group)
        
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Search:"))
        self.search_input = QLineEdit()
        self.search_input.textChanged.connect(self.filter_items)
        search_layout.addWidget(self.search_input)
        layout.addLayout(search_layout)
        
        self.loading_label = QLabel("Loading items...")
        self.loading_label.setStyleSheet("color: #ffa500; font-style: italic;")
        self.loading_label.hide()
        layout.addWidget(self.loading_label)
        
        self.tree_widget = QTreeWidget()
        self.tree_widget.setHeaderLabels(["Name", "Type", "Items"])
        self.tree_widget.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tree_widget.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree_widget.customContextMenuRequested.connect(self.show_context_menu)
        layout.addWidget(self.tree_widget)
        
        self.selection_info = QLabel("Selected: 0 items")
        layout.addWidget(self.selection_info)
        
        button_layout = QHBoxLayout()
        
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self.select_all)
        button_layout.addWidget(select_all_btn)
        
        clear_all_btn = QPushButton("Clear Selection")
        clear_all_btn.clicked.connect(self.clear_selection)
        button_layout.addWidget(clear_all_btn)
        
        button_layout.addStretch()
        
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        ok_btn.setStyleSheet("background-color: #00a8ff;")
        button_layout.addWidget(ok_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QGroupBox { color: #ffffff; border: 2px solid #555555; border-radius: 5px; margin-top: 10px; font-weight: bold; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }
            QRadioButton { color: #ffffff; }
            QTreeWidget { background-color: #1e1e1e; color: #ffffff; gridline-color: #555555; }
            QTreeWidget::item:selected { background-color: #00a8ff; }
            QHeaderView::section { background-color: #3c3c3c; color: #ffffff; padding: 5px; border: 1px solid #555555; }
            QLineEdit { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 5px; border-radius: 3px; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
        """)
        
        self.load_library_items()
    
    def load_library_items(self):
        self.tree_widget.clear()
        self.loading_label.show()
        self.search_input.setEnabled(False)
        
        self.playlist_radio.setEnabled(False)
        self.album_radio.setEnabled(False)
        self.artist_radio.setEnabled(False)
        
        self.loader_thread = LibraryLoaderThread(self.plex, self.selection_type)
        self.loader_thread.items_loaded.connect(self.on_items_loaded)
        self.loader_thread.error_occurred.connect(self.on_load_error)
        self.loader_thread.progress_update.connect(self.loading_label.setText)
        self.loader_thread.start()
    
    def on_items_loaded(self, items, selection_type):
        self.all_items = items
        self.tree_widget.clear()
        
        for item_data in items:
            item = QTreeWidgetItem([
                item_data['name'],
                item_data['type'].capitalize(),
                str(item_data['count'])
            ])
            item.setData(0, Qt.ItemDataRole.UserRole, {
                'type': item_data['type'],
                'object': item_data['object']
            })
            self.tree_widget.addTopLevelItem(item)
        
        for i in range(3):
            self.tree_widget.resizeColumnToContents(i)
        
        self.loading_label.hide()
        self.search_input.setEnabled(True)
        
        self.playlist_radio.setEnabled(True)
        self.album_radio.setEnabled(True)
        self.artist_radio.setEnabled(True)
        
        self.update_selection_info()
    
    def on_load_error(self, error_msg):
        self.loading_label.hide()
        self.search_input.setEnabled(True)
        self.playlist_radio.setEnabled(True)
        self.album_radio.setEnabled(True)
        self.artist_radio.setEnabled(True)
        QMessageBox.critical(self, "Error", error_msg)
    
    def on_type_changed(self, type_name):
        if self.selection_type != type_name:
            self.selection_type = type_name
            self.load_library_items()
            self.update_selection_info()
    
    def filter_items(self):
        search_text = self.search_input.text().lower()
        for i in range(self.tree_widget.topLevelItemCount()):
            item = self.tree_widget.topLevelItem(i)
            item.setHidden(search_text not in item.text(0).lower())
    
    def show_context_menu(self, position):
        menu = QMenu()
        select_action = QAction("Select", self)
        select_action.triggered.connect(lambda: self.select_items(True))
        menu.addAction(select_action)
        deselect_action = QAction("Deselect", self)
        deselect_action.triggered.connect(lambda: self.select_items(False))
        menu.addAction(deselect_action)
        menu.exec(self.tree_widget.viewport().mapToGlobal(position))
    
    def select_items(self, select=True):
        for item in self.tree_widget.selectedItems():
            item.setSelected(select)
        self.update_selection_info()
    
    def select_all(self):
        for i in range(self.tree_widget.topLevelItemCount()):
            item = self.tree_widget.topLevelItem(i)
            if not item.isHidden():
                item.setSelected(True)
        self.update_selection_info()
    
    def clear_selection(self):
        self.tree_widget.clearSelection()
        self.update_selection_info()
    
    def update_selection_info(self):
        count = len(self.tree_widget.selectedItems())
        self.selection_info.setText(f"Selected: {count} items")
    
    def get_selected_items(self):
        selected = []
        for item in self.tree_widget.selectedItems():
            data = item.data(0, Qt.ItemDataRole.UserRole)
            if data:
                selected.append(data)
        return selected


class SaveConfigDialog(QDialog):
    """Dialog for saving configuration"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.config_name = ""
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Save Configuration")
        self.setMinimumWidth(400)
        
        layout = QVBoxLayoutDialog()
        
        layout.addWidget(QLabel("Configuration Name:"))
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Enter a name for this configuration")
        layout.addWidget(self.name_input)
        
        layout.addWidget(QLabel("Description (optional):"))
        self.desc_input = QTextEdit()
        self.desc_input.setMaximumHeight(100)
        layout.addWidget(self.desc_input)
        
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        save_btn = QPushButton("Save")
        save_btn.clicked.connect(self.save_config)
        save_btn.setStyleSheet("background-color: #00a8ff;")
        button_layout.addWidget(save_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QLineEdit, QTextEdit { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 5px; border-radius: 3px; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
        """)
    
    def save_config(self):
        self.config_name = self.name_input.text().strip()
        if not self.config_name:
            QMessageBox.warning(self, "Invalid Name", "Please enter a configuration name")
            return
        self.accept()


class LoadConfigDialog(QDialog):
    """Dialog for loading configuration"""
    
    def __init__(self, configs, parent=None):
        super().__init__(parent)
        self.configs = configs
        self.selected_config = None
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Load Configuration")
        self.setMinimumSize(500, 400)
        
        layout = QVBoxLayoutDialog()
        layout.addWidget(QLabel("Select a configuration to load:"))
        
        self.config_list = QListWidget()
        for config in self.configs:
            item_text = f"{config['name']}"
            if 'description' in config and config['description']:
                item_text += f"\n  {config['description']}"
            if 'timestamp' in config:
                item_text += f"\n  Saved: {config['timestamp']}"
            
            item = QListWidgetItem(item_text)
            item.setData(Qt.ItemDataRole.UserRole, config)
            self.config_list.addItem(item)
        
        self.config_list.itemDoubleClicked.connect(self.accept)
        layout.addWidget(self.config_list)
        
        button_layout = QHBoxLayout()
        
        delete_btn = QPushButton("Delete Selected")
        delete_btn.clicked.connect(self.delete_config)
        button_layout.addWidget(delete_btn)
        
        button_layout.addStretch()
        
        load_btn = QPushButton("Load")
        load_btn.clicked.connect(self.load_selected)
        load_btn.setStyleSheet("background-color: #00a8ff;")
        button_layout.addWidget(load_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QListWidget { background-color: #1e1e1e; color: #ffffff; border: 1px solid #555555; }
            QListWidget::item { padding: 8px; border-bottom: 1px solid #3c3c3c; }
            QListWidget::item:selected { background-color: #00a8ff; }
            QListWidget::item:hover { background-color: #3c3c3c; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
        """)
    
    def load_selected(self):
        current_item = self.config_list.currentItem()
        if current_item:
            self.selected_config = current_item.data(Qt.ItemDataRole.UserRole)
            self.accept()
        else:
            QMessageBox.warning(self, "No Selection", "Please select a configuration to load")
    
    def delete_config(self):
        current_item = self.config_list.currentItem()
        if current_item:
            config = current_item.data(Qt.ItemDataRole.UserRole)
            reply = QMessageBox.question(self, "Confirm Delete", 
                f"Delete configuration '{config['name']}'?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                self.configs.remove(config)
                self.config_list.takeItem(self.config_list.row(current_item))


class PlexTidalMatcherGUI(QMainWindow):
    """Main GUI window"""
    
    def __init__(self):
        super().__init__()
        self.matches = []
        self.tidal_session = None
        self.worker = None
        self.login_thread = None
        self.auth_dialog = None
        self.plex_server = None
        self.selected_items = []
        self.saved_configs = []
        self.clear_thread = None
        self.config_file = "plex_tidal_configs.json"
        self.credentials_file = "plex_tidal_credentials.json"
        self._saved_plex_url = "http://localhost:32400"
        self._saved_plex_token = ""
        
        self._load_configs_from_file()
        self._load_credentials_from_file()
        self.init_ui()
        
        if self._saved_plex_url:
            self.plex_url_input.setText(self._saved_plex_url)
        if self._saved_plex_token:
            self.plex_token_input.setText(self._saved_plex_token)
        
        if self.auto_connect_check.isChecked() and self._saved_plex_token:
            QTimer.singleShot(500, self.auto_connect)
    
    def _load_configs_from_file(self):
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    self.saved_configs = json.load(f)
        except:
            self.saved_configs = []
    
    def _load_credentials_from_file(self):
        try:
            if os.path.exists(self.credentials_file):
                with open(self.credentials_file, 'r', encoding='utf-8') as f:
                    creds = json.load(f)
                    self._saved_plex_url = creds.get('plex_url', 'http://localhost:32400')
                    self._saved_plex_token = creds.get('plex_token', '')
        except:
            pass
    
    def init_ui(self):
        self.setWindowTitle("Plex Tidal Music Matcher")
        self.setGeometry(100, 100, 1400, 900)
        
        self.setStyleSheet("""
            QMainWindow { background-color: #2b2b2b; }
            QLabel { color: #ffffff; font-size: 12px; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
            QPushButton:disabled { background-color: #2a2a2a; color: #888888; }
            QLineEdit, QSpinBox { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 5px; border-radius: 3px; }
            QTextEdit { background-color: #1e1e1e; color: #00ff00; border: 1px solid #555555; font-family: 'Courier New', monospace; }
            QProgressBar { border: 1px solid #555555; border-radius: 3px; color: #ffffff; }
            QProgressBar::chunk { background-color: #00a8ff; }
            QGroupBox { color: #ffffff; border: 2px solid #555555; border-radius: 5px; margin-top: 10px; font-weight: bold; }
            QGroupBox::title { subcontrol-origin: margin; left: 10px; padding: 0 5px 0 5px; }
            QTableWidget { background-color: #1e1e1e; color: #ffffff; gridline-color: #555555; }
            QHeaderView::section { background-color: #3c3c3c; color: #ffffff; padding: 5px; border: 1px solid #555555; }
            QTabWidget::pane { border: 1px solid #555555; background-color: #2b2b2b; }
            QTabBar::tab { background-color: #3c3c3c; color: #ffffff; padding: 8px 16px; }
            QTabBar::tab:selected { background-color: #00a8ff; }
            QCheckBox { color: #ffffff; }
            QMenuBar { background-color: #3c3c3c; color: #ffffff; }
            QMenuBar::item:selected { background-color: #00a8ff; }
            QMenu { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; }
            QMenu::item:selected { background-color: #00a8ff; }
        """)
        
        menubar = self.menuBar()
        
        file_menu = menubar.addMenu("File")
        save_creds_action = QAction("Save Credentials", self)
        save_creds_action.triggered.connect(self.save_credentials)
        file_menu.addAction(save_creds_action)
        file_menu.addSeparator()
        
        save_config_action = QAction("Save Configuration", self)
        save_config_action.triggered.connect(self.save_configuration_dialog)
        file_menu.addAction(save_config_action)
        
        load_config_action = QAction("Load Configuration", self)
        load_config_action.triggered.connect(self.load_configuration_dialog)
        file_menu.addAction(load_config_action)
        file_menu.addSeparator()
        
        export_action = QAction("Export Results", self)
        export_action.triggered.connect(self.export_results)
        file_menu.addAction(export_action)
        file_menu.addSeparator()
        
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        tools_menu = menubar.addMenu("Tools")
        clear_ratings_action = QAction("Clear Plex Ratings", self)
        clear_ratings_action.triggered.connect(self.clear_ratings)
        tools_menu.addAction(clear_ratings_action)
        tools_menu.addSeparator()
        
        update_selected_action = QAction("Update Selected with Popularity", self)
        update_selected_action.triggered.connect(self.update_selected_ratings)
        tools_menu.addAction(update_selected_action)
        
        view_menu = menubar.addMenu("View")
        clear_log_action = QAction("Clear Log", self)
        clear_log_action.triggered.connect(lambda: self.log_output.clear())
        view_menu.addAction(clear_log_action)
        
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        
        splitter = QSplitter(Qt.Orientation.Vertical)
        main_layout.addWidget(splitter)
        
        config_widget = QWidget()
        config_layout = QVBoxLayout(config_widget)
        
        connection_group = QGroupBox("Connection Settings")
        connection_layout = QGridLayout()
        
        connection_layout.addWidget(QLabel("Plex Server URL:"), 0, 0)
        self.plex_url_input = QLineEdit("http://localhost:32400")
        connection_layout.addWidget(self.plex_url_input, 0, 1)
        
        connection_layout.addWidget(QLabel("Plex Token:"), 1, 0)
        self.plex_token_input = QLineEdit()
        self.plex_token_input.setEchoMode(QLineEdit.EchoMode.Password)
        connection_layout.addWidget(self.plex_token_input, 1, 1)
        
        get_token_btn = QPushButton("Get Plex Token Help")
        get_token_btn.clicked.connect(self.show_token_help)
        connection_layout.addWidget(get_token_btn, 1, 2)
        
        plex_btn_layout = QHBoxLayout()
        connect_plex_btn = QPushButton("Connect to Plex")
        connect_plex_btn.clicked.connect(self.connect_plex)
        plex_btn_layout.addWidget(connect_plex_btn)
        
        self.auto_connect_check = QCheckBox("Auto-connect on startup")
        self.auto_connect_check.setChecked(True)
        plex_btn_layout.addWidget(self.auto_connect_check)
        connection_layout.addLayout(plex_btn_layout, 2, 1, 1, 2)
        
        connect_tidal_btn = QPushButton("Connect & Login to Tidal")
        connect_tidal_btn.clicked.connect(self.connect_tidal)
        connection_layout.addWidget(connect_tidal_btn, 3, 0, 1, 3)
        
        self.connection_status = QLabel("Not connected")
        self.connection_status.setStyleSheet("color: #ff6b6b;")
        connection_layout.addWidget(self.connection_status, 4, 0, 1, 3)
        
        connection_group.setLayout(connection_layout)
        config_layout.addWidget(connection_group)
        
        selection_group = QGroupBox("Library Selection")
        selection_layout = QVBoxLayout()
        
        select_items_btn = QPushButton("Select Playlists/Albums/Artists")
        select_items_btn.clicked.connect(self.select_library_items)
        selection_layout.addWidget(select_items_btn)
        
        self.selection_status = QLabel("No items selected - will process entire library")
        self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
        selection_layout.addWidget(self.selection_status)
        
        selection_group.setLayout(selection_layout)
        config_layout.addWidget(selection_group)
        
        options_group = QGroupBox("Matching Options")
        options_layout = QGridLayout()
        
        options_layout.addWidget(QLabel("Match Threshold (%):"), 0, 0)
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(50, 100)
        self.threshold_spin.setValue(70)
        options_layout.addWidget(self.threshold_spin, 0, 1)
        
        self.limit_artists_check = QCheckBox("Limit number of artists to process")
        self.limit_artists_check.setChecked(True)
        options_layout.addWidget(self.limit_artists_check, 1, 0)
        
        options_layout.addWidget(QLabel("Artist Limit:"), 1, 1)
        self.artist_limit_spin = QSpinBox()
        self.artist_limit_spin.setRange(1, 1000)
        self.artist_limit_spin.setValue(50)
        options_layout.addWidget(self.artist_limit_spin, 1, 2)
        
        self.update_ratings_check = QCheckBox("Update Plex ratings based on popularity")
        self.update_ratings_check.setChecked(False)
        options_layout.addWidget(self.update_ratings_check, 2, 0, 1, 3)
        
        options_group.setLayout(options_layout)
        config_layout.addWidget(options_group)
        
        control_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("Start Matching")
        self.start_btn.clicked.connect(self.start_matching)
        self.start_btn.setEnabled(False)
        control_layout.addWidget(self.start_btn)
        
        self.stop_btn = QPushButton("Stop")
        self.stop_btn.clicked.connect(self.stop_matching)
        self.stop_btn.setEnabled(False)
        control_layout.addWidget(self.stop_btn)
        
        self.export_btn = QPushButton("Export Results")
        self.export_btn.clicked.connect(self.export_results)
        self.export_btn.setEnabled(False)
        control_layout.addWidget(self.export_btn)
        
        self.clear_ratings_btn = QPushButton("Clear Ratings")
        self.clear_ratings_btn.clicked.connect(self.clear_ratings)
        self.clear_ratings_btn.setStyleSheet("background-color: #8b0000;")
        self.clear_ratings_btn.setEnabled(False)
        control_layout.addWidget(self.clear_ratings_btn)
        
        control_layout.addStretch()
        config_layout.addLayout(control_layout)
        
        splitter.addWidget(config_widget)
        
        bottom_widget = QWidget()
        bottom_layout = QVBoxLayout(bottom_widget)
        
        self.progress_bar = QProgressBar()
        bottom_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("Ready")
        bottom_layout.addWidget(self.status_label)
        
        tabs = QTabWidget()
        
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(7)
        self.results_table.setHorizontalHeaderLabels([
            "Artist", "Track", "Album", "Popularity", "Match Score", "Rating", "Tidal URL"
        ])
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.results_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.results_table.customContextMenuRequested.connect(self.show_results_context_menu)
        results_layout.addWidget(self.results_table)
        
        tabs.addTab(results_widget, "Matches")
        
        log_widget = QWidget()
        log_layout = QVBoxLayout(log_widget)
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        log_layout.addWidget(self.log_output)
        
        clear_log_btn = QPushButton("Clear Log")
        clear_log_btn.clicked.connect(lambda: self.log_output.clear())
        log_layout.addWidget(clear_log_btn)
        
        tabs.addTab(log_widget, "Logs")
        
        bottom_layout.addWidget(tabs)
        splitter.addWidget(bottom_widget)
        
        splitter.setSizes([350, 550])
        
        self.log("Plex Tidal Music Matcher initialized")
    
    def log(self, message, level="INFO"):
        if hasattr(self, 'log_output'):
            self.log_output.append(message)
            cursor = self.log_output.textCursor()
            cursor.movePosition(QTextCursor.MoveOperation.End)
            self.log_output.setTextCursor(cursor)
    
    def auto_connect(self):
        if self._saved_plex_token:
            self.log("Auto-connecting to Plex...")
            self.connect_plex()
    
    def save_credentials(self):
        try:
            creds = {
                'plex_url': self.plex_url_input.text(),
                'plex_token': self.plex_token_input.text()
            }
            with open(self.credentials_file, 'w', encoding='utf-8') as f:
                json.dump(creds, f, indent=2, ensure_ascii=False)
            self.log("Credentials saved successfully")
            QMessageBox.information(self, "Saved", "Credentials saved successfully")
        except Exception as e:
            self.log(f"Error saving credentials: {str(e)}")
    
    def show_token_help(self):
        msg = QMessageBox()
        msg.setWindowTitle("Plex Token Help")
        msg.setText("To find your Plex token:\n\n"
                   "1. Open Plex Web App\n"
                   "2. Click the Settings icon (wrench)\n"
                   "3. Go to 'Manage' > 'Libraries'\n"
                   "4. Click on any library and select '...' > 'Manage Library' > 'Edit'\n"
                   "5. Click 'Add folders' and then 'Cancel'\n"
                   "6. In the URL, find 'X-Plex-Token=' and copy the value")
        msg.setIcon(QMessageBox.Icon.Information)
        msg.exec()
    
    def connect_plex(self):
        try:
            plex_url = self.plex_url_input.text().strip()
            plex_token = self.plex_token_input.text().strip()
            
            if not plex_url or not plex_token:
                QMessageBox.warning(self, "Missing Info", "Please enter Plex URL and Token")
                return
            
            self.log("Connecting to Plex server...")
            self.plex_server = PlexServer(plex_url, plex_token)
            
            try:
                server_info = self.plex_server.friendlyName
            except:
                server_info = plex_url
            
            self.log(f"Connected to Plex server: {server_info}")
            self.save_credentials()
            self.update_connection_status()
            
        except Exception as e:
            self.log(f"Plex connection error: {str(e)}")
            self.plex_server = None
            QMessageBox.critical(self, "Connection Error", f"Failed to connect to Plex:\n{str(e)}")
    
    def connect_tidal(self):
        try:
            self.log("Starting Tidal OAuth login...")
            self.connection_status.setText("Logging in to Tidal...")
            self.connection_status.setStyleSheet("color: #ffa500;")
            
            self.login_thread = TidalLoginThread()
            self.login_thread.log_signal.connect(self.log)
            self.login_thread.auth_url_signal.connect(self.show_auth_dialog)
            self.login_thread.login_success.connect(self.on_tidal_login_success)
            self.login_thread.login_error.connect(self.on_tidal_login_error)
            self.login_thread.start()
            
        except Exception as e:
            self.log(f"Connection error: {str(e)}")
    
    def show_auth_dialog(self, auth_url):
        self.auth_dialog = AuthDialog(auth_url, self)
        self.auth_dialog.exec()
    
    def on_tidal_login_success(self, session):
        self.tidal_session = session
        user = session.user
        self.log(f"Successfully logged in as: {user.username}")
        self.update_connection_status()
    
    def on_tidal_login_error(self, error_msg):
        self.log(f"Login failed: {error_msg}")
        self.connection_status.setText("Tidal login failed")
        self.connection_status.setStyleSheet("color: #ff6b6b;")
    
    def update_connection_status(self):
        plex_connected = self.plex_server is not None
        tidal_connected = self.tidal_session is not None
        
        if hasattr(self, 'clear_ratings_btn'):
            self.clear_ratings_btn.setEnabled(plex_connected)
        
        if plex_connected and tidal_connected:
            self.connection_status.setText("Connected to Plex and Tidal")
            self.connection_status.setStyleSheet("color: #51cf66;")
            self.start_btn.setEnabled(True)
        elif plex_connected:
            self.connection_status.setText("Connected to Plex only - Login to Tidal")
            self.connection_status.setStyleSheet("color: #ffa500;")
        elif tidal_connected:
            self.connection_status.setText("Connected to Tidal only - Connect to Plex")
            self.connection_status.setStyleSheet("color: #ffa500;")
        else:
            self.connection_status.setText("Not connected")
            self.connection_status.setStyleSheet("color: #ff6b6b;")
    
    def select_library_items(self):
        if not self.plex_server:
            QMessageBox.warning(self, "Not Connected", "Please connect to Plex first")
            return
        
        dialog = LibrarySelectorDialog(self.plex_server, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.selected_items = dialog.get_selected_items()
            if self.selected_items:
                type_counts = {}
                for item in self.selected_items:
                    item_type = item['type']
                    type_counts[item_type] = type_counts.get(item_type, 0) + 1
                status_parts = [f"{count} {item_type}{'s' if count > 1 else ''}" 
                               for item_type, count in type_counts.items()]
                self.selection_status.setText(f"Selected: {', '.join(status_parts)}")
                self.selection_status.setStyleSheet("color: #51cf66; font-style: normal;")
            else:
                self.selection_status.setText("No items selected - will process entire library")
                self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
    
    def save_configuration_dialog(self):
        dialog = SaveConfigDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.save_configuration(dialog.config_name, dialog.desc_input.toPlainText())
    
    def save_configuration(self, name, description=""):
        config = {
            'name': name,
            'description': description,
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'plex_url': self.plex_url_input.text(),
            'plex_token': self.plex_token_input.text(),
            'match_threshold': self.threshold_spin.value(),
            'limit_artists': self.limit_artists_check.isChecked(),
            'artist_limit': self.artist_limit_spin.value(),
            'update_ratings': self.update_ratings_check.isChecked(),
            'auto_connect': self.auto_connect_check.isChecked()
        }
        
        existing_index = None
        for i, saved_config in enumerate(self.saved_configs):
            if saved_config['name'] == name:
                existing_index = i
                break
        
        if existing_index is not None:
            reply = QMessageBox.question(self, "Overwrite?", 
                f"Configuration '{name}' already exists. Overwrite?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if reply == QMessageBox.StandardButton.Yes:
                self.saved_configs[existing_index] = config
            else:
                return
        else:
            self.saved_configs.append(config)
        
        with open(self.config_file, 'w', encoding='utf-8') as f:
            json.dump(self.saved_configs, f, indent=2, ensure_ascii=False)
        
        self.log(f"Configuration '{name}' saved")
        QMessageBox.information(self, "Saved", f"Configuration '{name}' saved successfully")
    
    def load_configuration_dialog(self):
        if not self.saved_configs:
            QMessageBox.information(self, "No Configs", "No saved configurations found")
            return
        
        dialog = LoadConfigDialog(self.saved_configs, self)
        if dialog.exec() == QDialog.DialogCode.Accepted and dialog.selected_config:
            self.load_configuration(dialog.selected_config)
            with open(self.config_file, 'w', encoding='utf-8') as f:
                json.dump(self.saved_configs, f, indent=2, ensure_ascii=False)
    
    def load_configuration(self, config):
        self.plex_url_input.setText(config.get('plex_url', 'http://localhost:32400'))
        self.plex_token_input.setText(config.get('plex_token', ''))
        self.threshold_spin.setValue(config.get('match_threshold', 70))
        self.limit_artists_check.setChecked(config.get('limit_artists', True))
        self.artist_limit_spin.setValue(config.get('artist_limit', 50))
        self.update_ratings_check.setChecked(config.get('update_ratings', False))
        self.auto_connect_check.setChecked(config.get('auto_connect', True))
        self.log(f"Loaded configuration: {config['name']}")
        if config.get('plex_token'):
            self.connect_plex()
    
    def clear_ratings(self):
        if not self.plex_server:
            QMessageBox.warning(self, "Not Connected", "Please connect to Plex first")
            return
        
        has_selection = bool(self.selected_items)
        dialog = ClearRatingsDialog(has_selection, self)
        
        if dialog.exec() != QDialog.DialogCode.Accepted:
            return
        
        scope = dialog.get_scope()
        clear_all = (scope == "all")
        selected = self.selected_items if scope == "selected" and has_selection else None
        
        self.clear_thread = ClearRatingsThread(
            self.plex_server,
            selected_items=selected,
            clear_all=clear_all
        )
        self.clear_thread.log_signal.connect(self.log)
        self.clear_thread.progress_signal.connect(self.update_progress)
        self.clear_thread.status_signal.connect(self.status_label.setText)
        self.clear_thread.finished_signal.connect(self.clear_ratings_finished)
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.stop_btn.setText("Stop Clearing")
        try:
            self.stop_btn.clicked.disconnect()
        except:
            pass
        self.stop_btn.clicked.connect(self.stop_clearing)
        
        self.clear_thread.start()
        self.log(f"Starting to clear ratings from {'all tracks' if clear_all else 'selected items'}...")
    
    def stop_clearing(self):
        if self.clear_thread:
            self.clear_thread.stop()
            self.log("Stopping clear ratings process...")
    
    def clear_ratings_finished(self, cleared_count):
        self.log(f"Clear ratings completed. Cleared {cleared_count} ratings")
        self.status_label.setText(f"Cleared {cleared_count} ratings")
        
        self.stop_btn.setText("Stop")
        self.stop_btn.setEnabled(False)
        try:
            self.stop_btn.clicked.disconnect()
        except:
            pass
        self.stop_btn.clicked.connect(self.stop_matching)
        
        if self.plex_server and self.tidal_session:
            self.start_btn.setEnabled(True)
        
        QMessageBox.information(self, "Complete", f"Successfully cleared ratings from {cleared_count} tracks")
    
    def update_selected_ratings(self):
        if not self.matches:
            QMessageBox.information(self, "No Matches", "No matches available to update ratings from")
            return
        
        reply = QMessageBox.question(self, "Update Ratings",
            f"Update Plex ratings for {len(self.matches)} matched tracks based on Tidal popularity?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            self.update_plex_ratings(self.matches)
    
    def update_plex_ratings(self, matches):
        try:
            self.log("Updating Plex ratings...")
            updated = 0
            for match in matches:
                try:
                    track = match['plex_track']
                    popularity = match['popularity']
                    rating = min(10, max(0, popularity / 10))
                    track.rate(rating)
                    updated += 1
                except:
                    pass
            
            self.log(f"Updated ratings for {updated} tracks")
            QMessageBox.information(self, "Ratings Updated", f"Successfully updated ratings for {updated} tracks")
        except Exception as e:
            self.log(f"Error updating ratings: {str(e)}")
    
    def start_matching(self):
        if not self.tidal_session or not self.plex_server:
            QMessageBox.warning(self, "Not Connected", "Please connect to both Plex and Tidal")
            return
        
        options = {
            'match_threshold': self.threshold_spin.value(),
            'limit_artists': self.limit_artists_check.isChecked(),
            'artist_limit': self.artist_limit_spin.value(),
            'update_ratings': self.update_ratings_check.isChecked()
        }
        
        self.matches = []
        self.results_table.setRowCount(0)
        
        self.worker = TidalPlexMatcher(
            self.plex_url_input.text().strip(),
            self.plex_token_input.text().strip(),
            self.tidal_session,
            options,
            self.selected_items if self.selected_items else None
        )
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.match_found_signal.connect(self.add_match_to_table)
        self.worker.status_signal.connect(self.status_label.setText)
        self.worker.finished_signal.connect(self.matching_finished)
        
        self.worker.start()
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.export_btn.setEnabled(False)
    
    def stop_matching(self):
        if self.worker:
            self.worker.stop()
            self.log("Stopping matching process...")
    
    def update_progress(self, current, total):
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
    
    def add_match_to_table(self, match_info):
        self.matches.append(match_info)
        
        row = self.results_table.rowCount()
        self.results_table.insertRow(row)
        
        track = match_info['plex_track']
        tidal_track = match_info['tidal_track']
        
        popularity = match_info['popularity']
        rating = min(5, max(0, popularity / 20))
        
        try:
            artist_name = track.artist().title if track.artist() else "Unknown"
            album_name = track.album().title if track.album() else "Unknown"
        except:
            artist_name = "Unknown"
            album_name = "Unknown"
        
        tidal_url = f"https://tidal.com/browse/track/{tidal_track.id}" if hasattr(tidal_track, 'id') else ""
        
        self.results_table.setItem(row, 0, QTableWidgetItem(artist_name))
        self.results_table.setItem(row, 1, QTableWidgetItem(track.title))
        self.results_table.setItem(row, 2, QTableWidgetItem(album_name))
        self.results_table.setItem(row, 3, QTableWidgetItem(f"{popularity}"))
        self.results_table.setItem(row, 4, QTableWidgetItem(f"{match_info['match_score']}%"))
        self.results_table.setItem(row, 5, QTableWidgetItem(f"{rating:.1f} ★"))
        self.results_table.setItem(row, 6, QTableWidgetItem(tidal_url))
        
        if popularity > 75:
            for col in range(7):
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(QColor(60, 80, 60))
        elif popularity > 50:
            for col in range(7):
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(QColor(80, 80, 60))
    
    def show_results_context_menu(self, position):
        menu = QMenu()
        
        open_url_action = QAction("Open in Tidal", self)
        open_url_action.triggered.connect(self.open_selected_tidal_url)
        menu.addAction(open_url_action)
        
        copy_action = QAction("Copy Track Info", self)
        copy_action.triggered.connect(self.copy_selected_track_info)
        menu.addAction(copy_action)
        
        menu.addSeparator()
        
        clear_rating_action = QAction("Clear Rating for Selected Track", self)
        clear_rating_action.triggered.connect(self.clear_selected_track_rating)
        menu.addAction(clear_rating_action)
        
        menu.exec(self.results_table.viewport().mapToGlobal(position))
    
    def open_selected_tidal_url(self):
        current_row = self.results_table.currentRow()
        if current_row >= 0:
            url_item = self.results_table.item(current_row, 6)
            if url_item and url_item.text():
                webbrowser.open(url_item.text())
    
    def copy_selected_track_info(self):
        current_row = self.results_table.currentRow()
        if current_row >= 0:
            artist = self.results_table.item(current_row, 0).text()
            track = self.results_table.item(current_row, 1).text()
            album = self.results_table.item(current_row, 2).text()
            info = f"{artist} - {track} ({album})"
            QApplication.clipboard().setText(info)
            self.log(f"Copied to clipboard: {info}")
    
    def clear_selected_track_rating(self):
        current_row = self.results_table.currentRow()
        if current_row >= 0 and current_row < len(self.matches):
            match = self.matches[current_row]
            try:
                track = match['plex_track']
                track.rate(None)
                self.results_table.setItem(current_row, 5, QTableWidgetItem("0.0 ★"))
                artist = self.results_table.item(current_row, 0).text()
                title = self.results_table.item(current_row, 1).text()
                self.log(f"Cleared rating for: {artist} - {title}")
            except Exception as e:
                self.log(f"Failed to clear rating: {str(e)}")
    
    def matching_finished(self, matches):
        self.log(f"Matching completed. Total matches: {len(matches)}")
        self.status_label.setText(f"Completed - {len(matches)} matches found")
        
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.export_btn.setEnabled(len(matches) > 0)
    
    def export_results(self):
        if not self.matches:
            return
        
        try:
            filename, _ = QFileDialog.getSaveFileName(
                self, "Save Results", 
                f"plex_tidal_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                "JSON Files (*.json)"
            )
            
            if not filename:
                return
            
            export_data = []
            for match in self.matches:
                try:
                    artist_name = match['plex_track'].artist().title if match['plex_track'].artist() else "Unknown"
                    album_name = match['plex_track'].album().title if match['plex_track'].album() else "Unknown"
                except:
                    artist_name = "Unknown"
                    album_name = "Unknown"
                
                export_data.append({
                    'artist': artist_name,
                    'track': match['plex_track'].title,
                    'album': album_name,
                    'popularity': match['popularity'],
                    'match_score': match['match_score'],
                    'tidal_id': match['tidal_track'].id if hasattr(match['tidal_track'], 'id') else None,
                    'tidal_url': f"https://tidal.com/browse/track/{match['tidal_track'].id}" if hasattr(match['tidal_track'], 'id') else None,
                    'rating': min(5, max(0, match['popularity'] / 20))
                })
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2, ensure_ascii=False)
            
            self.log(f"Results exported to {filename}")
            QMessageBox.information(self, "Export Complete", f"Results exported to:\n{filename}")
                
        except Exception as e:
            self.log(f"Export error: {str(e)}")
    
    def closeEvent(self, event):
        if self.plex_token_input.text():
            self.save_credentials()
        event.accept()


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = PlexTidalMatcherGUI()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()