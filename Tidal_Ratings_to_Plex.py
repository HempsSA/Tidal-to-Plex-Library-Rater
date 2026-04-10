#!/usr/bin/env python3
"""
Plex Tidal Music Matcher
Matches songs from Plex library with Tidal and rates them by popularity
Includes credential saving, duplicate detection, and library selection
"""

import sys
import os
import json
import time
import webbrowser
import base64
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# Check for required packages
try:
    from PyQt6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
        QPushButton, QLabel, QTextEdit, QProgressBar, QLineEdit,
        QComboBox, QCheckBox, QSpinBox, QGroupBox, QGridLayout,
        QMessageBox, QTabWidget, QTableWidget, QTableWidgetItem,
        QHeaderView, QSplitter, QFileDialog, QDialog,
        QListWidget, QListWidgetItem, QAbstractItemView, QRadioButton,
        QButtonGroup, QTreeWidget, QTreeWidgetItem, QMenu, QFrame
    )
    from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
    from PyQt6.QtGui import QTextCursor, QColor, QAction, QFont
except ImportError as e:
    print(f"ERROR: PyQt6 not installed. Run: pip install PyQt6")
    input("Press Enter to exit...")
    sys.exit(1)

try:
    from plexapi.server import PlexServer
except ImportError:
    print("ERROR: plexapi not installed. Run: pip install plexapi")
    input("Press Enter to exit...")
    sys.exit(1)

try:
    import tidalapi
except ImportError:
    print("ERROR: tidalapi not installed. Run: pip install tidalapi")
    input("Press Enter to exit...")
    sys.exit(1)

QVBoxLayoutDialog = QVBoxLayout

# Configuration file paths
CREDENTIALS_FILE = "plex_tidal_credentials.json"
CONFIG_FILE = "plex_tidal_config.json"


class LibraryLoaderThread(QThread):
    """Thread for loading library items"""
    
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
            
            # Find music section
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
                    for playlist in playlists:
                        if hasattr(playlist, 'playlistType') and playlist.playlistType == 'audio':
                            try:
                                item_count = len(playlist.items())
                            except:
                                item_count = "?"
                            
                            items.append({
                                'title': playlist.title,
                                'type': 'playlist',
                                'object': playlist,
                                'count': str(item_count)
                            })
                    self.progress_update.emit(f"Loaded {len(items)} playlists")
                except Exception as e:
                    self.error_occurred.emit(f"Could not load playlists: {str(e)}")
                    
            elif self.selection_type == "album":
                self.progress_update.emit("Loading albums...")
                try:
                    albums = list(music_section.albums())
                    for album in albums:
                        try:
                            track_count = len(album.tracks())
                        except:
                            track_count = "?"
                        
                        artist_name = album.parentTitle if hasattr(album, 'parentTitle') else "Unknown"
                        
                        items.append({
                            'title': album.title,
                            'artist': artist_name,
                            'type': 'album',
                            'object': album,
                            'count': str(track_count)
                        })
                    self.progress_update.emit(f"Loaded {len(items)} albums")
                except Exception as e:
                    self.error_occurred.emit(f"Could not load albums: {str(e)}")
                    
            elif self.selection_type == "artist":
                self.progress_update.emit("Loading artists...")
                try:
                    artists = list(music_section.all())
                    for artist in artists:
                        try:
                            album_count = len(artist.albums())
                        except:
                            album_count = "?"
                        
                        items.append({
                            'title': artist.title,
                            'type': 'artist',
                            'object': artist,
                            'count': str(album_count)
                        })
                    self.progress_update.emit(f"Loaded {len(items)} artists")
                except Exception as e:
                    self.error_occurred.emit(f"Could not load artists: {str(e)}")
            
            self.items_loaded.emit(items, self.selection_type)
            
        except Exception as e:
            self.error_occurred.emit(f"Error loading items: {str(e)}")


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
        self.load_items()
        
    def init_ui(self):
        self.setWindowTitle("Select Library Items")
        self.setMinimumSize(900, 600)
        
        layout = QVBoxLayoutDialog()
        
        # Selection type
        type_group = QGroupBox("Select Type")
        type_layout = QHBoxLayout()
        
        self.type_group = QButtonGroup()
        
        self.playlist_radio = QRadioButton("Playlists")
        self.playlist_radio.setChecked(True)
        self.playlist_radio.toggled.connect(lambda: self.on_type_changed("playlist"))
        self.type_group.addButton(self.playlist_radio)
        
        self.album_radio = QRadioButton("Albums")
        self.album_radio.toggled.connect(lambda: self.on_type_changed("album"))
        self.type_group.addButton(self.album_radio)
        
        self.artist_radio = QRadioButton("Artists")
        self.artist_radio.toggled.connect(lambda: self.on_type_changed("artist"))
        self.type_group.addButton(self.artist_radio)
        
        type_layout.addWidget(self.playlist_radio)
        type_layout.addWidget(self.album_radio)
        type_layout.addWidget(self.artist_radio)
        type_layout.addStretch()
        type_group.setLayout(type_layout)
        layout.addWidget(type_group)
        
        # Search
        search_layout = QHBoxLayout()
        search_layout.addWidget(QLabel("Search:"))
        self.search_input = QLineEdit()
        self.search_input.textChanged.connect(self.filter_items)
        search_layout.addWidget(self.search_input)
        layout.addLayout(search_layout)
        
        # Loading indicator
        self.loading_label = QLabel("Loading items...")
        self.loading_label.setStyleSheet("color: #ffa500; font-style: italic;")
        self.loading_label.hide()
        layout.addWidget(self.loading_label)
        
        # Tree widget
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Name", "Artist/Info", "Tracks"])
        self.tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.show_context_menu)
        layout.addWidget(self.tree)
        
        # Selection info
        self.selection_info = QLabel("Selected: 0 items")
        layout.addWidget(self.selection_info)
        
        # Quick select buttons
        quick_layout = QHBoxLayout()
        
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self.select_all)
        quick_layout.addWidget(select_all_btn)
        
        clear_btn = QPushButton("Clear All")
        clear_btn.clicked.connect(self.clear_selection)
        quick_layout.addWidget(clear_btn)
        
        invert_btn = QPushButton("Invert Selection")
        invert_btn.clicked.connect(self.invert_selection)
        quick_layout.addWidget(invert_btn)
        
        quick_layout.addStretch()
        layout.addLayout(quick_layout)
        
        # Separator
        line = QFrame()
        line.setFrameShape(QFrame.Shape.HLine)
        line.setStyleSheet("background-color: #555555;")
        layout.addWidget(line)
        
        # Action buttons
        button_layout = QHBoxLayout()
        button_layout.addStretch()
        
        ok_btn = QPushButton("OK")
        ok_btn.clicked.connect(self.accept)
        ok_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold; padding: 10px 30px;")
        button_layout.addWidget(ok_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        button_layout.addWidget(cancel_btn)
        
        layout.addLayout(button_layout)
        
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QGroupBox {
                color: #ffffff;
                border: 2px solid #555555;
                border-radius: 5px;
                margin-top: 10px;
                font-weight: bold;
                padding: 15px;
            }
            QRadioButton { color: #ffffff; padding: 5px; }
            QTreeWidget {
                background-color: #1e1e1e;
                color: #ffffff;
                gridline-color: #555555;
                font-size: 12px;
            }
            QTreeWidget::item { padding: 5px; }
            QTreeWidget::item:selected { background-color: #00a8ff; }
            QTreeWidget::item:hover { background-color: #3c3c3c; }
            QHeaderView::section {
                background-color: #3c3c3c;
                color: #ffffff;
                padding: 8px;
                border: 1px solid #555555;
            }
            QLineEdit {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
                padding: 8px;
                border-radius: 3px;
            }
            QPushButton {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
                padding: 8px 16px;
                border-radius: 4px;
            }
            QPushButton:hover { background-color: #4a4a4a; }
        """)
    
    def load_items(self):
        self.tree.clear()
        self.loading_label.show()
        self.search_input.setEnabled(False)
        
        self.playlist_radio.setEnabled(False)
        self.album_radio.setEnabled(False)
        self.artist_radio.setEnabled(False)
        
        self.loader_thread = LibraryLoaderThread(self.plex, self.selection_type)
        self.loader_thread.items_loaded.connect(self.on_items_loaded)
        self.loader_thread.error_occurred.connect(self.on_error)
        self.loader_thread.progress_update.connect(self.loading_label.setText)
        self.loader_thread.start()
    
    def on_items_loaded(self, items, selection_type):
        self.all_items = items
        self.tree.clear()
        
        for item_data in items:
            if selection_type == "playlist":
                item = QTreeWidgetItem([
                    item_data['title'],
                    "-",
                    item_data['count']
                ])
            elif selection_type == "album":
                item = QTreeWidgetItem([
                    item_data['title'],
                    item_data.get('artist', 'Unknown'),
                    item_data['count']
                ])
            else:  # artist
                item = QTreeWidgetItem([
                    item_data['title'],
                    f"{item_data['count']} albums",
                    "-"
                ])
            
            item.setData(0, Qt.ItemDataRole.UserRole, {
                'type': item_data['type'],
                'object': item_data['object'],
                'title': item_data['title']
            })
            self.tree.addTopLevelItem(item)
        
        for i in range(3):
            self.tree.resizeColumnToContents(i)
        
        self.loading_label.hide()
        self.search_input.setEnabled(True)
        
        self.playlist_radio.setEnabled(True)
        self.album_radio.setEnabled(True)
        self.artist_radio.setEnabled(True)
        
        self.update_selection_info()
    
    def on_error(self, error_msg):
        self.loading_label.hide()
        self.search_input.setEnabled(True)
        self.playlist_radio.setEnabled(True)
        self.album_radio.setEnabled(True)
        self.artist_radio.setEnabled(True)
        QMessageBox.critical(self, "Error", error_msg)
    
    def on_type_changed(self, type_name):
        if self.selection_type != type_name:
            self.selection_type = type_name
            self.load_items()
    
    def filter_items(self):
        search_text = self.search_input.text().lower()
        
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            match = search_text in item.text(0).lower()
            if not match and self.selection_type == "album":
                match = search_text in item.text(1).lower()
            item.setHidden(not match)
    
    def show_context_menu(self, position):
        menu = QMenu()
        
        select_action = QAction("Select", self)
        select_action.triggered.connect(lambda: self.set_selected(True))
        menu.addAction(select_action)
        
        deselect_action = QAction("Deselect", self)
        deselect_action.triggered.connect(lambda: self.set_selected(False))
        menu.addAction(deselect_action)
        
        menu.exec(self.tree.viewport().mapToGlobal(position))
    
    def set_selected(self, select=True):
        for item in self.tree.selectedItems():
            item.setSelected(select)
        self.update_selection_info()
    
    def select_all(self):
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if not item.isHidden():
                item.setSelected(True)
        self.update_selection_info()
    
    def clear_selection(self):
        self.tree.clearSelection()
        self.update_selection_info()
    
    def invert_selection(self):
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if not item.isHidden():
                item.setSelected(not item.isSelected())
        self.update_selection_info()
    
    def update_selection_info(self):
        count = len(self.tree.selectedItems())
        self.selection_info.setText(f"Selected: {count} items")
    
    def get_selected_items(self):
        selected = []
        for item in self.tree.selectedItems():
            data = item.data(0, Qt.ItemDataRole.UserRole)
            if data:
                selected.append(data)
        return selected


class DuplicateTrackHandler:
    """Handles duplicate track detection and prefers original release years"""
    
    @staticmethod
    def normalize_title(title: str) -> str:
        """Normalize track title by removing version suffixes"""
        title_lower = title.lower().strip()
        
        suffixes = [
            ' (remastered', ' (remaster', ' (deluxe', ' (expanded',
            ' (bonus track', ' (bonus)', ' (live)', ' (live',
            ' (acoustic)', ' (acoustic', ' (demo)', ' (demo',
            ' (single)', ' (single', ' (radio edit)', ' (radio edit',
            ' (album version)', ' (album version', ' - remastered',
            ' [remastered]', ' [live]', ' (original)', ' [original]'
        ]
        
        for suffix in suffixes:
            if suffix in title_lower:
                title_lower = title_lower.split(suffix)[0].strip()
        
        while title_lower.endswith(')') or title_lower.endswith(']'):
            if '(' in title_lower:
                title_lower = title_lower[:title_lower.rfind('(')].strip()
            elif '[' in title_lower:
                title_lower = title_lower[:title_lower.rfind('[')].strip()
            else:
                break
        
        return title_lower
    
    @staticmethod
    def get_album_year(track) -> Optional[int]:
        """Get the release year of a track's album"""
        try:
            if hasattr(track, 'album') and track.album():
                album = track.album()
                
                if hasattr(album, 'year') and album.year:
                    return int(album.year)
                elif hasattr(album, 'originallyAvailableAt') and album.originallyAvailableAt:
                    return album.originallyAvailableAt.year
                elif hasattr(album, 'releaseDate') and album.releaseDate:
                    if isinstance(album.releaseDate, str):
                        return int(album.releaseDate[:4])
                    else:
                        return album.releaseDate.year
        except:
            pass
        return None
    
    @staticmethod
    def score_track_version(track) -> Tuple[int, Optional[int], str]:
        """Score a track version - higher score = better (more likely original)"""
        score = 0
        year = DuplicateTrackHandler.get_album_year(track)
        title_lower = track.title.lower()
        album_name = ""
        
        try:
            if track.album():
                album_name = track.album().title.lower()
        except:
            pass
        
        if year:
            score += (2030 - year) * 10
        else:
            score -= 1000
        
        bad_words = ['remaster', 'deluxe', 'expanded', 'bonus', 'live', 
                    'acoustic', 'demo', 'alternate', 'instrumental', 'karaoke']
        for word in bad_words:
            if word in title_lower:
                score -= 500
            if word in album_name:
                score -= 300
        
        good_words = ['original', 'first pressing', 'standard']
        for word in good_words:
            if word in title_lower or word in album_name:
                score += 200
        
        score -= len(title_lower) * 2
        
        return (score, year, title_lower)
    
    @staticmethod
    def group_tracks_by_title_and_artist(tracks: List) -> Dict[str, List]:
        """Group tracks by normalized title and artist"""
        groups = {}
        
        for track in tracks:
            try:
                artist = track.artist().title.lower().strip() if track.artist() else "unknown"
                title = DuplicateTrackHandler.normalize_title(track.title)
                key = f"{artist}|||{title}"
                
                if key not in groups:
                    groups[key] = []
                groups[key].append(track)
            except:
                continue
                
        return groups
    
    @staticmethod
    def select_best_version(tracks: List):
        """Select the best version from duplicate tracks"""
        if len(tracks) <= 1:
            return tracks[0] if tracks else None
        
        scored = [(DuplicateTrackHandler.score_track_version(t), t) for t in tracks]
        scored.sort(key=lambda x: x[0], reverse=True)
        
        return scored[0][1] if scored else tracks[0]
    
    @staticmethod
    def deduplicate_tracks(tracks: List, log_callback=None) -> List:
        """Remove duplicates, keeping only the best version of each song"""
        if not tracks:
            return []
        
        groups = DuplicateTrackHandler.group_tracks_by_title_and_artist(tracks)
        
        unique_tracks = []
        duplicates_removed = 0
        
        for key, group in groups.items():
            if len(group) > 1:
                duplicates_removed += len(group) - 1
                best = DuplicateTrackHandler.select_best_version(group)
                unique_tracks.append(best)
                
                if log_callback:
                    try:
                        artist, title = key.split('|||', 1)
                        year = DuplicateTrackHandler.get_album_year(best)
                        album = best.album().title if best.album() else "Unknown"
                        log_callback(f"  Selected: {artist} - {title} ({album}, {year}) from {len(group)} versions")
                    except:
                        pass
            else:
                unique_tracks.append(group[0])
        
        if log_callback and duplicates_removed > 0:
            log_callback(f"Deduplication: {len(tracks)} -> {len(unique_tracks)} tracks ({duplicates_removed} duplicates removed)")
        
        return unique_tracks


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
            self.log_signal.emit("Waiting for authentication (2 minutes max)...")
            
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


class MatchingThread(QThread):
    """Worker thread for matching Plex songs with Tidal"""
    
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    match_found_signal = pyqtSignal(dict)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(list)
    
    def __init__(self, plex_server, tidal_session, options, selected_items=None):
        super().__init__()
        self.plex_server = plex_server
        self.tidal_session = tidal_session
        self.options = options
        self.selected_items = selected_items
        self.is_running = True
        self.matches = []
        
    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_signal.emit(f"[{timestamp}] {message}")
        
    def run(self):
        try:
            self.log("Starting track collection...")
            self.status_signal.emit("Collecting tracks...")
            
            all_tracks = []
            
            # If specific items are selected, only process those
            if self.selected_items:
                self.log(f"Processing {len(self.selected_items)} selected items...")
                for item_data in self.selected_items:
                    if not self.is_running:
                        break
                    
                    try:
                        if item_data['type'] == 'playlist':
                            playlist = item_data['object']
                            self.log(f"Loading playlist: {playlist.title}")
                            tracks = list(playlist.items())
                            all_tracks.extend(tracks)
                            self.log(f"  Added {len(tracks)} tracks from playlist")
                            
                        elif item_data['type'] == 'album':
                            album = item_data['object']
                            self.log(f"Loading album: {album.title}")
                            tracks = list(album.tracks())
                            all_tracks.extend(tracks)
                            self.log(f"  Added {len(tracks)} tracks from album")
                            
                        elif item_data['type'] == 'artist':
                            artist = item_data['object']
                            self.log(f"Loading artist: {artist.title}")
                            tracks = list(artist.tracks())
                            all_tracks.extend(tracks)
                            self.log(f"  Added {len(tracks)} tracks from artist")
                    except Exception as e:
                        self.log(f"Error loading item: {str(e)}")
            else:
                # Process entire library
                music_sections = [s for s in self.plex_server.library.sections() if s.type == 'artist']
                if not music_sections:
                    self.log("No music library found!")
                    return
                
                music_section = music_sections[0]
                self.log(f"Found music library: {music_section.title}")
                
                if self.options.get('limit_artists', False):
                    limit = self.options.get('artist_limit', 50)
                    artists = list(music_section.all())[:limit]
                    for artist in artists:
                        try:
                            artist_tracks = list(artist.tracks())
                            all_tracks.extend(artist_tracks)
                            self.log(f"Loaded {len(artist_tracks)} tracks from {artist.title}")
                        except:
                            pass
                else:
                    try:
                        for album in music_section.albums():
                            try:
                                all_tracks.extend(album.tracks())
                            except:
                                pass
                    except Exception as e:
                        self.log(f"Error fetching albums: {str(e)}")
                        return
            
            original_count = len(all_tracks)
            self.log(f"Found {original_count} total tracks")
            
            # Apply deduplication if enabled
            if self.options.get('deduplicate', True):
                self.log("Deduplicating tracks (keeping original/best versions)...")
                all_tracks = DuplicateTrackHandler.deduplicate_tracks(all_tracks, self.log)
            
            total_tracks = len(all_tracks)
            self.log(f"Processing {total_tracks} tracks")
            
            # Process tracks
            matches = []
            for idx, track in enumerate(all_tracks):
                if not self.is_running:
                    break
                    
                self.progress_signal.emit(idx + 1, total_tracks)
                
                try:
                    artist_name = track.artist().title if track.artist() else "Unknown"
                except:
                    continue
                
                self.status_signal.emit(f"Processing: {track.title} by {artist_name}")
                
                if idx % 10 == 0:
                    self.log(f"Processing {idx + 1}/{total_tracks}: {track.title}")
                
                # Search Tidal
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
                    
                    # Update rating if requested
                    if self.options.get('update_ratings', False):
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
            self.log(f"Error: {str(e)}")
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
                if score > best_score and score >= self.options.get('match_threshold', 70):
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


class AuthDialog(QDialog):
    """Dialog to show authentication URL"""
    
    def __init__(self, auth_url, parent=None):
        super().__init__(parent)
        self.auth_url = auth_url
        self.init_ui()
        
    def init_ui(self):
        self.setWindowTitle("Tidal Authentication")
        self.setMinimumWidth(600)
        self.setMinimumHeight(250)
        
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
        continue_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold;")
        layout.addWidget(continue_btn)
        
        self.setLayout(layout)
        
        self.setStyleSheet("""
            QDialog { background-color: #2b2b2b; }
            QLabel { color: #ffffff; }
            QPushButton {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
                padding: 10px;
                border-radius: 4px;
            }
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
            QMessageBox.warning(self, "Error", f"Could not open browser: {str(e)}")


class PlexTidalMatcherGUI(QMainWindow):
    """Main GUI window"""
    
    def __init__(self):
        super().__init__()
        self.matches = []
        self.tidal_session = None
        self.plex_server = None
        self.worker = None
        self.login_thread = None
        self.auth_dialog = None
        self.selected_items = []
        
        self.load_credentials()
        self.init_ui()
        
        if self.auto_connect_check.isChecked() and self._saved_plex_token:
            QTimer.singleShot(500, self.auto_connect)
    
    def load_credentials(self):
        """Load saved credentials"""
        self._saved_plex_url = "http://localhost:32400"
        self._saved_plex_token = ""
        
        try:
            if os.path.exists(CREDENTIALS_FILE):
                with open(CREDENTIALS_FILE, 'r') as f:
                    creds = json.load(f)
                    self._saved_plex_url = creds.get('plex_url', 'http://localhost:32400')
                    self._saved_plex_token = creds.get('plex_token', '')
        except:
            pass
    
    def save_credentials(self):
        """Save credentials to file"""
        try:
            creds = {
                'plex_url': self.plex_url_input.text().strip(),
                'plex_token': self.plex_token_input.text().strip()
            }
            with open(CREDENTIALS_FILE, 'w') as f:
                json.dump(creds, f, indent=2)
            self.log("✓ Credentials saved")
        except Exception as e:
            self.log(f"✗ Error saving credentials: {e}")
    
    def init_ui(self):
        self.setWindowTitle("🎵 Plex Tidal Music Matcher")
        self.setGeometry(100, 100, 1300, 850)
        
        self.setStyleSheet("""
            QMainWindow { background-color: #2b2b2b; }
            QLabel { color: #ffffff; font-size: 12px; }
            QPushButton {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
                padding: 10px;
                border-radius: 4px;
            }
            QPushButton:hover { background-color: #4a4a4a; }
            QPushButton:disabled { background-color: #2a2a2a; color: #888888; }
            QLineEdit, QSpinBox {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
                padding: 8px;
                border-radius: 3px;
            }
            QTextEdit {
                background-color: #1e1e1e;
                color: #00ff00;
                border: 1px solid #555555;
                font-family: 'Courier New', monospace;
                font-size: 11px;
            }
            QProgressBar {
                border: 1px solid #555555;
                border-radius: 3px;
                text-align: center;
                color: #ffffff;
            }
            QProgressBar::chunk { background-color: #00a8ff; border-radius: 2px; }
            QGroupBox {
                color: #ffffff;
                border: 2px solid #555555;
                border-radius: 5px;
                margin-top: 10px;
                font-weight: bold;
                padding: 15px;
            }
            QGroupBox::title {
                subcontrol-origin: margin;
                left: 10px;
                padding: 0 5px;
            }
            QTableWidget {
                background-color: #1e1e1e;
                color: #ffffff;
                gridline-color: #555555;
            }
            QHeaderView::section {
                background-color: #3c3c3c;
                color: #ffffff;
                padding: 8px;
                border: 1px solid #555555;
            }
            QTabWidget::pane {
                border: 1px solid #555555;
                background-color: #2b2b2b;
            }
            QTabBar::tab {
                background-color: #3c3c3c;
                color: #ffffff;
                padding: 10px 20px;
            }
            QTabBar::tab:selected { background-color: #00a8ff; }
            QCheckBox { color: #ffffff; }
            QMenuBar { background-color: #3c3c3c; color: #ffffff; }
            QMenuBar::item:selected { background-color: #00a8ff; }
            QMenu {
                background-color: #3c3c3c;
                color: #ffffff;
                border: 1px solid #555555;
            }
            QMenu::item:selected { background-color: #00a8ff; }
        """)
        
        # Menu bar
        menubar = self.menuBar()
        
        file_menu = menubar.addMenu("File")
        
        save_creds_action = QAction("Save Credentials", self)
        save_creds_action.triggered.connect(self.save_credentials)
        file_menu.addAction(save_creds_action)
        
        export_action = QAction("Export Results", self)
        export_action.triggered.connect(self.export_results)
        file_menu.addAction(export_action)
        
        file_menu.addSeparator()
        
        exit_action = QAction("Exit", self)
        exit_action.triggered.connect(self.close)
        file_menu.addAction(exit_action)
        
        view_menu = menubar.addMenu("View")
        clear_log_action = QAction("Clear Log", self)
        clear_log_action.triggered.connect(lambda: self.log_output.clear())
        view_menu.addAction(clear_log_action)
        
        # Central widget
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        
        # Connection group
        conn_group = QGroupBox("Connection Settings")
        conn_layout = QGridLayout()
        
        conn_layout.addWidget(QLabel("Plex Server URL:"), 0, 0)
        self.plex_url_input = QLineEdit(self._saved_plex_url)
        conn_layout.addWidget(self.plex_url_input, 0, 1)
        
        conn_layout.addWidget(QLabel("Plex Token:"), 1, 0)
        self.plex_token_input = QLineEdit(self._saved_plex_token)
        self.plex_token_input.setEchoMode(QLineEdit.EchoMode.Password)
        conn_layout.addWidget(self.plex_token_input, 1, 1)
        
        help_btn = QPushButton("How to get token")
        help_btn.clicked.connect(self.show_token_help)
        conn_layout.addWidget(help_btn, 1, 2)
        
        plex_btn_layout = QHBoxLayout()
        
        self.plex_btn = QPushButton("Connect to Plex")
        self.plex_btn.clicked.connect(self.connect_plex)
        plex_btn_layout.addWidget(self.plex_btn)
        
        self.auto_connect_check = QCheckBox("Auto-connect on startup")
        self.auto_connect_check.setChecked(True)
        plex_btn_layout.addWidget(self.auto_connect_check)
        
        conn_layout.addLayout(plex_btn_layout, 2, 1, 1, 2)
        
        self.tidal_btn = QPushButton("Login to Tidal")
        self.tidal_btn.clicked.connect(self.connect_tidal)
        conn_layout.addWidget(self.tidal_btn, 3, 1)
        
        self.status_label = QLabel("Not connected")
        self.status_label.setStyleSheet("color: #ff6b6b; padding: 5px;")
        conn_layout.addWidget(self.status_label, 4, 0, 1, 3)
        
        conn_group.setLayout(conn_layout)
        main_layout.addWidget(conn_group)
        
        # Selection group
        selection_group = QGroupBox("Library Selection")
        selection_layout = QHBoxLayout()
        
        self.select_btn = QPushButton("🎵 Select Playlists/Albums/Artists")
        self.select_btn.clicked.connect(self.select_library_items)
        self.select_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold; padding: 12px;")
        selection_layout.addWidget(self.select_btn)
        
        self.selection_status = QLabel("No items selected - will process entire library")
        self.selection_status.setStyleSheet("color: #888888; font-style: italic; padding: 5px;")
        selection_layout.addWidget(self.selection_status)
        
        selection_layout.addStretch()
        selection_group.setLayout(selection_layout)
        main_layout.addWidget(selection_group)
        
        # Options group
        options_group = QGroupBox("Matching Options")
        options_layout = QGridLayout()
        
        options_layout.addWidget(QLabel("Match Threshold (%):"), 0, 0)
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(50, 100)
        self.threshold_spin.setValue(70)
        self.threshold_spin.setToolTip("Minimum match confidence required")
        options_layout.addWidget(self.threshold_spin, 0, 1)
        
        self.limit_check = QCheckBox("Limit number of artists to process")
        self.limit_check.setChecked(False)
        options_layout.addWidget(self.limit_check, 1, 0)
        
        options_layout.addWidget(QLabel("Artist Limit:"), 1, 1)
        self.limit_spin = QSpinBox()
        self.limit_spin.setRange(1, 1000)
        self.limit_spin.setValue(50)
        options_layout.addWidget(self.limit_spin, 1, 2)
        
        self.deduplicate_check = QCheckBox("Deduplicate tracks (keep only original/best version)")
        self.deduplicate_check.setChecked(True)
        self.deduplicate_check.setToolTip("When multiple versions exist, only process the original/best version")
        options_layout.addWidget(self.deduplicate_check, 2, 0, 1, 3)
        
        self.update_ratings_check = QCheckBox("Update Plex ratings automatically")
        self.update_ratings_check.setChecked(False)
        options_layout.addWidget(self.update_ratings_check, 3, 0, 1, 3)
        
        options_group.setLayout(options_layout)
        main_layout.addWidget(options_group)
        
        # Control buttons
        btn_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("▶ Start Matching")
        self.start_btn.clicked.connect(self.start_matching)
        self.start_btn.setEnabled(False)
        self.start_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold; padding: 12px;")
        btn_layout.addWidget(self.start_btn)
        
        self.stop_btn = QPushButton("⬛ Stop")
        self.stop_btn.clicked.connect(self.stop_matching)
        self.stop_btn.setEnabled(False)
        btn_layout.addWidget(self.stop_btn)
        
        self.clear_btn = QPushButton("🗑️ Clear Selection")
        self.clear_btn.clicked.connect(self.clear_selection)
        self.clear_btn.setEnabled(False)
        btn_layout.addWidget(self.clear_btn)
        
        main_layout.addLayout(btn_layout)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        main_layout.addWidget(self.progress_bar)
        
        self.progress_label = QLabel("Ready")
        self.progress_label.setStyleSheet("color: #888888;")
        main_layout.addWidget(self.progress_label)
        
        # Tabs
        tabs = QTabWidget()
        
        # Results tab
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(5)
        self.results_table.setHorizontalHeaderLabels(["Artist", "Track", "Album", "Popularity", "Rating"])
        self.results_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.results_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.results_table.customContextMenuRequested.connect(self.show_context_menu)
        results_layout.addWidget(self.results_table)
        
        tabs.addTab(results_widget, "Matches")
        
        # Log tab
        log_widget = QWidget()
        log_layout = QVBoxLayout(log_widget)
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        log_layout.addWidget(self.log_output)
        
        tabs.addTab(log_widget, "Log")
        
        main_layout.addWidget(tabs)
        
        self.log("🎵 Plex Tidal Music Matcher initialized")
        self.log("Enter Plex credentials and connect, then login to Tidal")
        self.log("Use 'Select Playlists/Albums/Artists' to process specific items")
    
    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.append(f"[{timestamp}] {message}")
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.log_output.setTextCursor(cursor)
        QApplication.processEvents()
    
    def show_token_help(self):
        msg = QMessageBox()
        msg.setWindowTitle("Plex Token Help")
        msg.setText(
            "To find your Plex token:\n\n"
            "1. Open Plex Web App and sign in\n"
            "2. Click on any media item\n"
            "3. Click the three dots (...) and select 'Get Info'\n"
            "4. Click 'View XML'\n"
            "5. Look for 'X-Plex-Token=' in the URL\n\n"
            "Copy the long string of letters and numbers after the equals sign."
        )
        msg.setIcon(QMessageBox.Icon.Information)
        msg.exec()
    
    def auto_connect(self):
        if self._saved_plex_token:
            self.log("Auto-connecting to Plex...")
            self.connect_plex()
    
    def connect_plex(self):
        try:
            url = self.plex_url_input.text().strip()
            token = self.plex_token_input.text().strip()
            
            if not url or not token:
                QMessageBox.warning(self, "Missing Info", "Please enter Plex URL and Token")
                return
            
            self.log("Connecting to Plex...")
            self.plex_server = PlexServer(url, token)
            
            name = self.plex_server.friendlyName
            self.log(f"✓ Connected to Plex: {name}")
            
            self.save_credentials()
            
            self.plex_btn.setEnabled(False)
            self.plex_url_input.setEnabled(False)
            self.plex_token_input.setEnabled(False)
            
            self.select_btn.setEnabled(True)
            
            self.update_status()
            
        except Exception as e:
            self.log(f"✗ Plex connection failed: {e}")
            QMessageBox.critical(self, "Connection Error", str(e))
    
    def connect_tidal(self):
        try:
            self.log("Starting Tidal login...")
            self.tidal_btn.setEnabled(False)
            
            self.login_thread = TidalLoginThread()
            self.login_thread.log_signal.connect(self.log)
            self.login_thread.auth_url_signal.connect(self.show_auth_dialog)
            self.login_thread.login_success.connect(self.on_tidal_success)
            self.login_thread.login_error.connect(self.on_tidal_error)
            self.login_thread.start()
            
        except Exception as e:
            self.log(f"✗ Tidal connection error: {e}")
            self.tidal_btn.setEnabled(True)
    
    def show_auth_dialog(self, auth_url):
        self.auth_dialog = AuthDialog(auth_url, self)
        self.auth_dialog.exec()
    
    def on_tidal_success(self, session):
        self.tidal_session = session
        self.log(f"✓ Logged in as: {session.user.username}")
        self.tidal_btn.setEnabled(False)
        self.update_status()
    
    def on_tidal_error(self, error_msg):
        self.log(f"✗ Tidal login failed: {error_msg}")
        self.tidal_btn.setEnabled(True)
        QMessageBox.critical(self, "Login Error", error_msg)
    
    def update_status(self):
        if self.plex_server and self.tidal_session:
            self.status_label.setText("✓ Connected to Plex and Tidal - Ready!")
            self.status_label.setStyleSheet("color: #51cf66; padding: 5px; font-weight: bold;")
            self.start_btn.setEnabled(True)
        elif self.plex_server:
            self.status_label.setText("✓ Connected to Plex - Login to Tidal")
            self.status_label.setStyleSheet("color: #ffa500; padding: 5px;")
        elif self.tidal_session:
            self.status_label.setText("✓ Connected to Tidal - Connect to Plex")
            self.status_label.setStyleSheet("color: #ffa500; padding: 5px;")
    
    def select_library_items(self):
        if not self.plex_server:
            QMessageBox.warning(self, "Not Connected", "Please connect to Plex first")
            return
        
        dialog = LibrarySelectorDialog(self.plex_server, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.selected_items = dialog.get_selected_items()
            
            if self.selected_items:
                type_counts = {}
                total_tracks = 0
                
                for item in self.selected_items:
                    item_type = item['type']
                    type_counts[item_type] = type_counts.get(item_type, 0) + 1
                    
                    try:
                        if item_type == 'playlist':
                            total_tracks += len(item['object'].items())
                        elif item_type == 'album':
                            total_tracks += len(item['object'].tracks())
                        elif item_type == 'artist':
                            total_tracks += len(item['object'].tracks())
                    except:
                        pass
                
                status_parts = [f"{count} {item_type}{'s' if count > 1 else ''}" 
                               for item_type, count in type_counts.items()]
                
                self.selection_status.setText(f"Selected: {', '.join(status_parts)} (~{total_tracks} tracks)")
                self.selection_status.setStyleSheet("color: #51cf66; font-style: normal; font-weight: bold;")
                self.clear_btn.setEnabled(True)
                
                self.log(f"Selected {len(self.selected_items)} items: {', '.join(status_parts)}")
                self.log(f"  Estimated {total_tracks} tracks to process")
            else:
                self.selection_status.setText("No items selected - will process entire library")
                self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
                self.clear_btn.setEnabled(False)
    
    def clear_selection(self):
        self.selected_items = []
        self.selection_status.setText("No items selected - will process entire library")
        self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
        self.clear_btn.setEnabled(False)
        self.log("Selection cleared - will process entire library")
    
    def start_matching(self):
        if not self.plex_server or not self.tidal_session:
            return
        
        options = {
            'match_threshold': self.threshold_spin.value(),
            'limit_artists': self.limit_check.isChecked() and not self.selected_items,
            'artist_limit': self.limit_spin.value(),
            'deduplicate': self.deduplicate_check.isChecked(),
            'update_ratings': self.update_ratings_check.isChecked()
        }
        
        self.matches = []
        self.results_table.setRowCount(0)
        
        self.worker = MatchingThread(
            self.plex_server, 
            self.tidal_session, 
            options,
            self.selected_items if self.selected_items else None
        )
        self.worker.log_signal.connect(self.log)
        self.worker.progress_signal.connect(self.update_progress)
        self.worker.match_found_signal.connect(self.add_match)
        self.worker.status_signal.connect(self.progress_label.setText)
        self.worker.finished_signal.connect(self.matching_finished)
        
        self.worker.start()
        
        self.start_btn.setEnabled(False)
        self.stop_btn.setEnabled(True)
        self.select_btn.setEnabled(False)
        self.clear_btn.setEnabled(False)
    
    def stop_matching(self):
        if self.worker:
            self.worker.stop()
            self.log("Stopping...")
    
    def update_progress(self, current, total):
        self.progress_bar.setMaximum(total)
        self.progress_bar.setValue(current)
    
    def add_match(self, match_info):
        self.matches.append(match_info)
        
        row = self.results_table.rowCount()
        self.results_table.insertRow(row)
        
        track = match_info['plex_track']
        popularity = match_info['popularity']
        rating = min(5, max(0, popularity / 20))
        
        try:
            artist = track.artist().title if track.artist() else "Unknown"
            album = track.album().title if track.album() else "Unknown"
        except:
            artist = "Unknown"
            album = "Unknown"
        
        self.results_table.setItem(row, 0, QTableWidgetItem(artist))
        self.results_table.setItem(row, 1, QTableWidgetItem(track.title))
        self.results_table.setItem(row, 2, QTableWidgetItem(album))
        self.results_table.setItem(row, 3, QTableWidgetItem(str(popularity)))
        self.results_table.setItem(row, 4, QTableWidgetItem(f"{rating:.1f} ★"))
        
        if popularity > 75:
            for col in range(5):
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(QColor(60, 80, 60))
        elif popularity > 50:
            for col in range(5):
                item = self.results_table.item(row, col)
                if item:
                    item.setBackground(QColor(80, 80, 60))
    
    def show_context_menu(self, position):
        menu = QMenu()
        
        copy_action = QAction("Copy Track Info", self)
        copy_action.triggered.connect(self.copy_selected)
        menu.addAction(copy_action)
        
        menu.exec(self.results_table.viewport().mapToGlobal(position))
    
    def copy_selected(self):
        row = self.results_table.currentRow()
        if row >= 0:
            artist = self.results_table.item(row, 0).text()
            track = self.results_table.item(row, 1).text()
            QApplication.clipboard().setText(f"{artist} - {track}")
            self.log(f"Copied: {artist} - {track}")
    
    def matching_finished(self, matches):
        self.log(f"✓ Matching completed! Found {len(matches)} matches")
        self.progress_label.setText(f"Complete - {len(matches)} matches found")
        
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.select_btn.setEnabled(True)
        
        if self.selected_items:
            self.clear_btn.setEnabled(True)
        
        if matches:
            QMessageBox.information(self, "Complete", 
                f"Matching completed!\nFound {len(matches)} matches.")
    
    def export_results(self):
        if not self.matches:
            QMessageBox.information(self, "No Results", "No matches to export")
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
                    artist = match['plex_track'].artist().title if match['plex_track'].artist() else "Unknown"
                    album = match['plex_track'].album().title if match['plex_track'].album() else "Unknown"
                except:
                    artist = "Unknown"
                    album = "Unknown"
                
                export_data.append({
                    'artist': artist,
                    'track': match['plex_track'].title,
                    'album': album,
                    'popularity': match['popularity'],
                    'match_score': match['match_score'],
                    'rating': min(5, max(0, match['popularity'] / 20))
                })
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2)
            
            self.log(f"✓ Exported {len(export_data)} matches to {filename}")
            QMessageBox.information(self, "Export Complete", f"Exported to:\n{filename}")
            
        except Exception as e:
            self.log(f"✗ Export error: {e}")
    
    def closeEvent(self, event):
        if self.plex_token_input.text():
            self.save_credentials()
        event.accept()


def main():
    """Main entry point"""
    print("Starting Plex Tidal Matcher...")
    
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    
    window = PlexTidalMatcherGUI()
    window.show()
    
    print("Window displayed.")
    
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())