#!/usr/bin/env python3
"""
Plex Tidal Music Matcher - Enhanced & Optimized Edition
Faster library loading with caching and batch operations
"""

import sys
import os
import json
import time
import webbrowser
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
        QButtonGroup, QTreeWidget, QTreeWidgetItem, QMenu, QFrame,
        QScrollArea
    )
    from PyQt6.QtCore import Qt, QThread, pyqtSignal, QTimer
    from PyQt6.QtGui import QTextCursor, QColor, QAction, QKeySequence, QShortcut
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

# Configuration files
CREDENTIALS_FILE = "plex_tidal_credentials.json"
PROGRESS_FILE = "matching_progress.json"
THEME_FILE = "theme_settings.json"
TIDAL_SESSION_FILE = "tidal_session.json"
LIBRARY_FILE = "selected_library.json"


# ============================================================================
# THEME MANAGER
# ============================================================================

class ThemeManager:
    THEMES = {
        'dark': """
            QMainWindow { background-color: #2b2b2b; }
            QLabel { color: #ffffff; font-size: 12px; }
            QPushButton { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 10px; border-radius: 4px; }
            QPushButton:hover { background-color: #4a4a4a; }
            QPushButton:disabled { background-color: #2a2a2a; color: #888888; }
            QLineEdit, QSpinBox { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; padding: 8px; border-radius: 3px; }
            QTextEdit { background-color: #1e1e1e; color: #00ff00; border: 1px solid #555555; }
            QProgressBar { border: 1px solid #555555; border-radius: 3px; color: #ffffff; }
            QProgressBar::chunk { background-color: #00a8ff; }
            QGroupBox { color: #ffffff; border: 2px solid #555555; border-radius: 5px; margin-top: 10px; font-weight: bold; padding: 15px; }
            QTableWidget { background-color: #1e1e1e; color: #ffffff; gridline-color: #555555; }
            QHeaderView::section { background-color: #3c3c3c; color: #ffffff; padding: 8px; border: 1px solid #555555; }
            QTabWidget::pane { border: 1px solid #555555; background-color: #2b2b2b; }
            QTabBar::tab { background-color: #3c3c3c; color: #ffffff; padding: 10px 20px; }
            QTabBar::tab:selected { background-color: #00a8ff; }
            QCheckBox { color: #ffffff; }
            QMenuBar { background-color: #3c3c3c; color: #ffffff; }
            QMenuBar::item:selected { background-color: #00a8ff; }
            QMenu { background-color: #3c3c3c; color: #ffffff; border: 1px solid #555555; }
            QMenu::item:selected { background-color: #00a8ff; }
            QTreeWidget { background-color: #1e1e1e; color: #ffffff; }
            QTreeWidget::item:selected { background-color: #00a8ff; }
            QRadioButton { color: #ffffff; }
            QDialog { background-color: #2b2b2b; }
            QListWidget { background-color: #1e1e1e; color: #ffffff; }
        """,
        'light': """
            QMainWindow { background-color: #f5f5f5; }
            QLabel { color: #333333; font-size: 12px; }
            QPushButton { background-color: #e0e0e0; color: #333333; border: 1px solid #cccccc; padding: 10px; border-radius: 4px; }
            QPushButton:hover { background-color: #d0d0d0; }
            QLineEdit, QSpinBox { background-color: #ffffff; color: #333333; border: 1px solid #cccccc; padding: 8px; border-radius: 3px; }
            QTextEdit { background-color: #ffffff; color: #006600; border: 1px solid #cccccc; }
            QProgressBar { border: 1px solid #cccccc; }
            QProgressBar::chunk { background-color: #0078d4; }
            QGroupBox { color: #333333; border: 2px solid #cccccc; }
            QTableWidget { background-color: #ffffff; color: #333333; }
            QHeaderView::section { background-color: #e0e0e0; color: #333333; }
            QTabBar::tab { background-color: #e0e0e0; color: #333333; }
            QTabBar::tab:selected { background-color: #0078d4; color: white; }
            QCheckBox { color: #333333; }
            QMenuBar { background-color: #e0e0e0; color: #333333; }
            QRadioButton { color: #333333; }
            QDialog { background-color: #f5f5f5; }
            QListWidget { background-color: #ffffff; color: #333333; }
            QTreeWidget { background-color: #ffffff; color: #333333; }
            QTreeWidget::item:selected { background-color: #0078d4; color: white; }
        """
    }
    
    @classmethod
    def get_themes(cls):
        return list(cls.THEMES.keys())
    
    @classmethod
    def get_theme(cls, theme_name):
        return cls.THEMES.get(theme_name, cls.THEMES['dark'])
    
    @classmethod
    def save_theme_preference(cls, theme_name):
        try:
            with open(THEME_FILE, 'w') as f:
                json.dump({'theme': theme_name}, f)
        except:
            pass
    
    @classmethod
    def load_theme_preference(cls):
        try:
            if os.path.exists(THEME_FILE):
                with open(THEME_FILE, 'r') as f:
                    data = json.load(f)
                    return data.get('theme', 'dark')
        except:
            pass
        return 'dark'


# ============================================================================
# PROGRESS MANAGER
# ============================================================================

class ProgressManager:
    @staticmethod
    def save_progress(matches, filename=PROGRESS_FILE):
        try:
            progress = {
                'timestamp': datetime.now().isoformat(),
                'matches_count': len(matches),
                'matches': []
            }
            for match in matches:
                try:
                    track = match['plex_track']
                    progress['matches'].append({
                        'artist': track.artist().title if track.artist() else 'Unknown',
                        'track': track.title,
                        'album': track.album().title if track.album() else 'Unknown',
                        'popularity': match['popularity'],
                        'rating': min(5, max(0, match['popularity'] / 20))
                    })
                except:
                    pass
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(progress, f, indent=2, ensure_ascii=False)
            return True
        except:
            return False
    
    @staticmethod
    def load_progress(filename=PROGRESS_FILE):
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except:
            pass
        return None
    
    @staticmethod
    def clear_progress(filename=PROGRESS_FILE):
        try:
            if os.path.exists(filename):
                os.remove(filename)
        except:
            pass


# ============================================================================
# SMART PLAYLIST CREATOR
# ============================================================================

class SmartPlaylistCreator:
    @staticmethod
    def create_popularity_playlists(plex_server, matches, log_callback=None):
        tiers = {
            "🔥 Top Hits (80-100)": [],
            "⭐ Popular (60-79)": [],
            "👍 Good (40-59)": [],
            "📀 Deep Cuts (0-39)": []
        }
        for match in matches:
            popularity = match['popularity']
            track = match['plex_track']
            if popularity >= 80:
                tiers["🔥 Top Hits (80-100)"].append(track)
            elif popularity >= 60:
                tiers["⭐ Popular (60-79)"].append(track)
            elif popularity >= 40:
                tiers["👍 Good (40-59)"].append(track)
            else:
                tiers["📀 Deep Cuts (0-39)"].append(track)
        
        created = []
        for name, tracks in tiers.items():
            if tracks:
                try:
                    for p in plex_server.playlists():
                        if p.title == name:
                            p.delete()
                    plex_server.createPlaylist(name, items=tracks[:500])
                    created.append((name, len(tracks[:500])))
                    if log_callback:
                        log_callback(f"✓ Created '{name}' with {len(tracks[:500])} tracks")
                except Exception as e:
                    if log_callback:
                        log_callback(f"✗ Failed to create '{name}': {e}")
        return created


# ============================================================================
# AUDIO FORMAT HANDLER (ALAC Support)
# ============================================================================

class AudioFormatHandler:
    CODECS = {
        'alac': {'name': 'ALAC', 'lossless': True, 'bit_depth': '16-24bit'},
        'flac': {'name': 'FLAC', 'lossless': True, 'bit_depth': '16-24bit'},
        'wav': {'name': 'WAV', 'lossless': True, 'bit_depth': '16-24bit'},
        'aiff': {'name': 'AIFF', 'lossless': True, 'bit_depth': '16-24bit'},
        'dsd': {'name': 'DSD', 'lossless': True, 'bit_depth': '1bit'},
        'mp3': {'name': 'MP3', 'lossless': False, 'bit_depth': 'lossy'},
        'aac': {'name': 'AAC', 'lossless': False, 'bit_depth': 'lossy'},
        'ogg': {'name': 'OGG', 'lossless': False, 'bit_depth': 'lossy'},
        'm4a': {'name': 'M4A', 'lossless': False, 'bit_depth': 'lossy'},
        'opus': {'name': 'Opus', 'lossless': False, 'bit_depth': 'lossy'},
    }
    
    @classmethod
    def get_track_codec(cls, track):
        try:
            if hasattr(track, 'media'):
                for media in track.media:
                    for part in media.parts:
                        if hasattr(part, 'audioCodec'):
                            return part.audioCodec.lower()
            if hasattr(track, 'locations') and track.locations:
                ext = os.path.splitext(track.locations[0])[1].lower()
                return ext[1:] if ext else 'unknown'
        except:
            pass
        return 'unknown'
    
    @classmethod
    def is_lossless(cls, track):
        codec = cls.get_track_codec(track)
        return cls.CODECS.get(codec, {}).get('lossless', False)
    
    @classmethod
    def get_codec_info(cls, track):
        codec = cls.get_track_codec(track)
        return cls.CODECS.get(codec, {'name': codec.upper(), 'lossless': False, 'bit_depth': 'unknown'})
    
    @classmethod
    def filter_lossless(cls, tracks):
        return [t for t in tracks if cls.is_lossless(t)]
    
    @classmethod
    def get_format_stats(cls, tracks):
        stats = {}
        for track in tracks:
            codec = cls.get_track_codec(track)
            info = cls.CODECS.get(codec, {'name': codec.upper(), 'lossless': False})
            name = info['name']
            if name not in stats:
                stats[name] = {'count': 0, 'lossless': info['lossless']}
            stats[name]['count'] += 1
        return stats


# ============================================================================
# OPTIMIZED LIBRARY LOADER
# ============================================================================

class OptimizedLibraryLoader:
    """Faster Plex library loading with caching and batch operations"""
    
    def __init__(self, plex_server):
        self.plex = plex_server
        self.cache = {}
        self.batch_size = 100
        
    def get_all_tracks_fast(self, library, track_filter=None):
        """Get all tracks from a library using optimized methods"""
        cache_key = f"tracks_{library.key if hasattr(library, 'key') else str(library)}_{hash(str(track_filter))}"
        
        # Check cache first
        if cache_key in self.cache:
            return self.cache[cache_key]
        
        tracks = []
        
        try:
            # Method 1: Use search with limit and offset for pagination
            offset = 0
            while True:
                try:
                    batch = self.plex.library.search(
                        libtype='track',
                        limit=self.batch_size,
                        offset=offset
                    )
                    
                    if not batch:
                        break
                        
                    tracks.extend(batch)
                    offset += self.batch_size
                    
                    # Early exit if we have enough tracks and no filter
                    if track_filter is None and len(tracks) >= 10000:
                        break
                except:
                    break
                    
            # Method 2: Fallback to album iteration if search doesn't work
            if not tracks:
                for album in library.albums():
                    try:
                        album_tracks = list(album.tracks())
                        tracks.extend(album_tracks)
                    except:
                        continue
                        
        except Exception as e:
            # Fallback to original method
            for album in library.albums():
                try:
                    album_tracks = list(album.tracks())
                    tracks.extend(album_tracks)
                except:
                    continue
        
        # Cache the results
        self.cache[cache_key] = tracks
        return tracks
    
    def clear_cache(self):
        """Clear the cache to free memory"""
        self.cache.clear()


# ============================================================================
# FAST LIBRARY LOADER THREAD
# ============================================================================

class FastLibraryLoaderThread(QThread):
    items_loaded = pyqtSignal(list, str)
    error_occurred = pyqtSignal(str)
    progress_update = pyqtSignal(str)
    
    def __init__(self, plex_server, selection_type, specific_library=None):
        super().__init__()
        self.plex = plex_server
        self.selection_type = selection_type
        self.specific_library = specific_library
        self.loader = OptimizedLibraryLoader(plex_server)
        
    def run(self):
        try:
            items = []
            
            if self.specific_library:
                music_section = self.specific_library
            else:
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
                            items.append({
                                'title': playlist.title,
                                'type': 'playlist',
                                'object': playlist,
                                'count': '?'
                            })
                    self.progress_update.emit(f"Loaded {len(items)} playlists")
                except Exception as e:
                    self.error_occurred.emit(f"Could not load playlists: {str(e)}")
                    
            elif self.selection_type == "album":
                self.progress_update.emit(f"Loading albums from '{music_section.title}'...")
                try:
                    albums = music_section.albums()
                    album_list = []
                    for album in albums:
                        album_list.append(album)
                        if len(album_list) >= 500:
                            break
                    
                    for album in album_list:
                        artist_name = album.parentTitle if hasattr(album, 'parentTitle') else "Unknown"
                        items.append({
                            'title': album.title,
                            'artist': artist_name,
                            'type': 'album',
                            'object': album,
                            'count': '?'
                        })
                    self.progress_update.emit(f"Loaded {len(items)} albums")
                except Exception as e:
                    self.error_occurred.emit(f"Could not load albums: {str(e)}")
                    
            elif self.selection_type == "artist":
                self.progress_update.emit(f"Loading artists from '{music_section.title}'...")
                try:
                    artists = music_section.all()
                    artist_list = []
                    for artist in artists:
                        artist_list.append(artist)
                        if len(artist_list) >= 500:
                            break
                    
                    for artist in artist_list:
                        items.append({
                            'title': artist.title,
                            'type': 'artist',
                            'object': artist,
                            'count': '?'
                        })
                    self.progress_update.emit(f"Loaded {len(items)} artists")
                except Exception as e:
                    self.error_occurred.emit(f"Could not load artists: {str(e)}")
            
            self.items_loaded.emit(items, self.selection_type)
        except Exception as e:
            self.error_occurred.emit(f"Error loading items: {str(e)}")


# ============================================================================
# FAST LIBRARY SELECTOR DIALOG
# ============================================================================

class FastLibrarySelectorDialog(QDialog):
    def __init__(self, plex_server, parent=None, specific_library=None):
        super().__init__(parent)
        self.plex = plex_server
        self.specific_library = specific_library
        self.selected_items = []
        self.selection_type = "playlist"
        self.all_items = []
        self.loader_thread = None
        self.init_ui()
        self.load_items()
        
    def init_ui(self):
        self.setWindowTitle("Select Library Items (Fast Mode)")
        self.setMinimumSize(900, 600)
        layout = QVBoxLayoutDialog()
        
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
        
        self.tree = QTreeWidget()
        self.tree.setHeaderLabels(["Name", "Info"])
        self.tree.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        layout.addWidget(self.tree)
        
        self.selection_info = QLabel("Selected: 0 items")
        layout.addWidget(self.selection_info)
        
        quick_layout = QHBoxLayout()
        select_all_btn = QPushButton("Select All")
        select_all_btn.clicked.connect(self.select_all)
        quick_layout.addWidget(select_all_btn)
        clear_btn = QPushButton("Clear All")
        clear_btn.clicked.connect(self.clear_selection)
        quick_layout.addWidget(clear_btn)
        quick_layout.addStretch()
        layout.addLayout(quick_layout)
        
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
    
    def load_items(self):
        self.tree.clear()
        self.loading_label.show()
        self.search_input.setEnabled(False)
        
        self.loader_thread = FastLibraryLoaderThread(self.plex, self.selection_type, self.specific_library)
        self.loader_thread.items_loaded.connect(self.on_items_loaded)
        self.loader_thread.error_occurred.connect(self.on_error)
        self.loader_thread.progress_update.connect(self.loading_label.setText)
        self.loader_thread.start()
    
    def on_items_loaded(self, items, selection_type):
        self.all_items = items
        self.tree.clear()
        
        for item_data in items:
            if selection_type == "playlist":
                item = QTreeWidgetItem([item_data['title'], f"{item_data['count']} tracks"])
            elif selection_type == "album":
                item = QTreeWidgetItem([item_data['title'], f"{item_data.get('artist', 'Unknown')} ({item_data['count']} tracks)"])
            else:
                item = QTreeWidgetItem([item_data['title'], f"{item_data['count']} albums"])
            
            item.setData(0, Qt.ItemDataRole.UserRole, {
                'type': item_data['type'],
                'object': item_data['object'],
                'title': item_data['title']
            })
            self.tree.addTopLevelItem(item)
        
        self.tree.resizeColumnToContents(0)
        self.loading_label.hide()
        self.search_input.setEnabled(True)
        self.update_selection_info()
    
    def on_error(self, error_msg):
        self.loading_label.hide()
        self.search_input.setEnabled(True)
        QMessageBox.critical(self, "Error", error_msg)
    
    def on_type_changed(self, type_name):
        if self.selection_type != type_name:
            self.selection_type = type_name
            self.load_items()
    
    def filter_items(self):
        search_text = self.search_input.text().lower()
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            item.setHidden(search_text not in item.text(0).lower())
    
    def select_all(self):
        for i in range(self.tree.topLevelItemCount()):
            item = self.tree.topLevelItem(i)
            if not item.isHidden():
                item.setSelected(True)
        self.update_selection_info()
    
    def clear_selection(self):
        self.tree.clearSelection()
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


# ============================================================================
# FORMAT FILTER DIALOG
# ============================================================================

class FormatFilterDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("🎵 Audio Format Filter")
        self.setMinimumSize(400, 350)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayoutDialog()
        
        layout.addWidget(QLabel("<h3>Filter by Audio Format</h3>"))
        layout.addWidget(QLabel("<hr>"))
        
        self.lossless_only = QCheckBox("🎵 Lossless Only (ALAC, FLAC, WAV, AIFF, DSD)")
        self.lossless_only.setChecked(False)
        layout.addWidget(self.lossless_only)
        
        layout.addWidget(QLabel("<br><b>Specific Formats:</b>"))
        
        self.format_checks = {}
        formats = [
            ('alac', '🍎 ALAC (Apple Lossless)'),
            ('flac', '🎵 FLAC (Free Lossless)'),
            ('wav', '📀 WAV (Uncompressed)'),
            ('aiff', '🍎 AIFF (Apple Uncompressed)'),
            ('mp3', '🎵 MP3 (Lossy)'),
            ('aac', '🎵 AAC (Lossy)'),
            ('m4a', '🍎 M4A (AAC/ALAC)'),
        ]
        
        for codec, label in formats:
            cb = QCheckBox(label)
            cb.setChecked(False)
            self.format_checks[codec] = cb
            layout.addWidget(cb)
        
        layout.addWidget(QLabel("<hr>"))
        
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        
        apply_btn = QPushButton("Apply Filter")
        apply_btn.clicked.connect(self.accept)
        apply_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold;")
        btn_layout.addWidget(apply_btn)
        
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        
        layout.addLayout(btn_layout)
        self.setLayout(layout)
    
    def get_selected_formats(self):
        selected = []
        for codec, cb in self.format_checks.items():
            if cb.isChecked():
                selected.append(codec)
        return selected
    
    def is_lossless_only(self):
        return self.lossless_only.isChecked()


# ============================================================================
# STATISTICS DIALOG
# ============================================================================

class StatsDialog(QDialog):
    def __init__(self, matches, parent=None):
        super().__init__(parent)
        self.matches = matches
        self.setWindowTitle("📊 Library Statistics")
        self.setMinimumSize(600, 450)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayoutDialog()
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        total_tracks = len(self.matches)
        avg_popularity = sum(m['popularity'] for m in self.matches) / total_tracks if total_tracks else 0
        
        summary = QLabel(f"""
        <h2>📊 Library Statistics</h2>
        <hr>
        <p><b>Total Matched Tracks:</b> {total_tracks}</p>
        <p><b>Average Popularity:</b> {avg_popularity:.1f}</p>
        <hr>
        <h3>Popularity Distribution:</h3>
        """)
        summary.setWordWrap(True)
        scroll_layout.addWidget(summary)
        
        tiers = {'🔥 Top Hits (80-100)': 0, '⭐ Popular (60-79)': 0, 
                 '👍 Good (40-59)': 0, '📀 Deep Cuts (0-39)': 0}
        artist_tracks = {}
        
        for match in self.matches:
            pop = match['popularity']
            if pop >= 80:
                tiers['🔥 Top Hits (80-100)'] += 1
            elif pop >= 60:
                tiers['⭐ Popular (60-79)'] += 1
            elif pop >= 40:
                tiers['👍 Good (40-59)'] += 1
            else:
                tiers['📀 Deep Cuts (0-39)'] += 1
            try:
                artist = match['plex_track'].artist().title if match['plex_track'].artist() else 'Unknown'
                artist_tracks[artist] = artist_tracks.get(artist, 0) + 1
            except:
                pass
        
        for tier, count in tiers.items():
            pct = (count / total_tracks * 100) if total_tracks else 0
            scroll_layout.addWidget(QLabel(f"  {tier}: {count} tracks ({pct:.1f}%)"))
        
        scroll_layout.addWidget(QLabel("<hr><h3>Top 10 Artists:</h3>"))
        sorted_artists = sorted(artist_tracks.items(), key=lambda x: x[1], reverse=True)[:10]
        for i, (artist, count) in enumerate(sorted_artists, 1):
            scroll_layout.addWidget(QLabel(f"  {i}. {artist}: {count} tracks"))
        
        scroll_layout.addStretch()
        scroll.setWidget(scroll_widget)
        layout.addWidget(scroll)
        
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        close_btn.setStyleSheet("background-color: #00a8ff; padding: 10px;")
        layout.addWidget(close_btn)
        self.setLayout(layout)


# ============================================================================
# TIDAL ACCOUNT DIALOG
# ============================================================================

class TidalAccountDialog(QDialog):
    def __init__(self, tidal_session, parent=None):
        super().__init__(parent)
        self.session = tidal_session
        self.setWindowTitle("🌊 Tidal Account")
        self.setMinimumSize(450, 300)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayoutDialog()
        try:
            user = self.session.user
            layout.addWidget(QLabel(f"<h2>🌊 {user.username}</h2>"))
            layout.addWidget(QLabel("<hr>"))
            info = QLabel(f"""
            <p><b>Email:</b> {user.email}</p>
            <p><b>Country:</b> {user.country}</p>
            <p><b>Account ID:</b> {user.id}</p>
            <p><b>Subscription:</b> {user.subscription.type.capitalize()}</p>
            """)
            info.setWordWrap(True)
            layout.addWidget(info)
        except Exception as e:
            layout.addWidget(QLabel(f"<p style='color: #ff6b6b;'>Error: {e}</p>"))
        
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        close_btn.setStyleSheet("background-color: #00a8ff; padding: 10px;")
        layout.addWidget(close_btn)
        self.setLayout(layout)


# ============================================================================
# PLAYLIST CREATION DIALOG
# ============================================================================

class PlaylistCreationDialog(QDialog):
    def __init__(self, plex_server, matches, parent=None):
        super().__init__(parent)
        self.plex = plex_server
        self.matches = matches
        self.setWindowTitle("🎵 Create Smart Playlists")
        self.setMinimumSize(450, 300)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayoutDialog()
        layout.addWidget(QLabel(f"<h3>Create Smart Playlists</h3>"))
        layout.addWidget(QLabel(f"<p>Based on {len(self.matches)} matched tracks</p>"))
        layout.addWidget(QLabel("<hr>"))
        
        self.log_output = QTextEdit()
        self.log_output.setReadOnly(True)
        self.log_output.setMaximumHeight(200)
        layout.addWidget(self.log_output)
        
        create_btn = QPushButton("🎵 Create Popularity Playlists")
        create_btn.clicked.connect(self.create_playlists)
        create_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold; padding: 12px;")
        layout.addWidget(create_btn)
        
        close_btn = QPushButton("Close")
        close_btn.clicked.connect(self.accept)
        layout.addWidget(close_btn)
        self.setLayout(layout)
    
    def log(self, msg):
        self.log_output.append(msg)
        QApplication.processEvents()
    
    def create_playlists(self):
        self.log_output.clear()
        self.log("Creating playlists...")
        SmartPlaylistCreator.create_popularity_playlists(self.plex, self.matches, self.log)
        self.log("\n✅ Done!")


# ============================================================================
# PROGRESS DIALOG
# ============================================================================

class ProgressDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("📁 Progress Management")
        self.setMinimumSize(500, 300)
        self.init_ui()
        
    def init_ui(self):
        layout = QVBoxLayoutDialog()
        progress = ProgressManager.load_progress()
        
        if progress:
            layout.addWidget(QLabel(f"<h3>Saved Progress Found</h3>"))
            layout.addWidget(QLabel(f"<b>Saved:</b> {progress.get('timestamp', 'Unknown')}"))
            layout.addWidget(QLabel(f"<b>Matches:</b> {progress.get('matches_count', 0)} tracks"))
            layout.addWidget(QLabel("<hr>"))
            
            btn_layout = QHBoxLayout()
            load_btn = QPushButton("📂 Load Progress")
            load_btn.clicked.connect(self.accept)
            load_btn.setStyleSheet("background-color: #00a8ff;")
            btn_layout.addWidget(load_btn)
            
            clear_btn = QPushButton("🗑️ Clear Progress")
            clear_btn.clicked.connect(self.clear_progress)
            clear_btn.setStyleSheet("background-color: #8b0000;")
            btn_layout.addWidget(clear_btn)
            
            cancel_btn = QPushButton("Cancel")
            cancel_btn.clicked.connect(self.reject)
            btn_layout.addWidget(cancel_btn)
            layout.addLayout(btn_layout)
        else:
            layout.addWidget(QLabel("<h3>No Saved Progress</h3>"))
            layout.addWidget(QLabel("<p>No previous matching progress found.</p>"))
            close_btn = QPushButton("Close")
            close_btn.clicked.connect(self.reject)
            layout.addWidget(close_btn)
        
        self.setLayout(layout)
    
    def clear_progress(self):
        reply = QMessageBox.question(self, "Confirm", "Clear saved progress?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            ProgressManager.clear_progress()
            self.reject()


# ============================================================================
# LIBRARY SELECTOR
# ============================================================================

class LibrarySelector(QDialog):
    def __init__(self, plex_server, parent=None):
        super().__init__(parent)
        self.plex = plex_server
        self.selected_library = None
        self.music_libraries = []
        self.init_ui()
        self.load_libraries()
        
    def init_ui(self):
        self.setWindowTitle("Select Music Library")
        self.setMinimumSize(500, 300)
        layout = QVBoxLayoutDialog()
        layout.addWidget(QLabel("<h3>Select Music Library</h3>"))
        layout.addWidget(QLabel("<p>Choose which Plex music library to use:</p>"))
        layout.addWidget(QLabel("<hr>"))
        
        self.library_list = QListWidget()
        self.library_list.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.library_list.itemDoubleClicked.connect(self.accept)
        layout.addWidget(self.library_list)
        
        self.info_label = QLabel("")
        self.info_label.setStyleSheet("color: #888888; font-style: italic; padding: 5px;")
        layout.addWidget(self.info_label)
        
        btn_layout = QHBoxLayout()
        btn_layout.addStretch()
        select_btn = QPushButton("Select Library")
        select_btn.clicked.connect(self.accept)
        select_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold;")
        btn_layout.addWidget(select_btn)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btn_layout.addWidget(cancel_btn)
        layout.addLayout(btn_layout)
        
        self.setLayout(layout)
        self.library_list.itemSelectionChanged.connect(self.update_info)
    
    def load_libraries(self):
        try:
            sections = self.plex.library.sections()
            for section in sections:
                if section.type == 'artist':
                    self.music_libraries.append(section)
                    try:
                        artist_count = len(section.all())
                        album_count = len(section.albums())
                        item_text = f"{section.title}\n  Artists: {artist_count} | Albums: {album_count}"
                    except:
                        item_text = section.title
                    item = QListWidgetItem(item_text)
                    item.setData(Qt.ItemDataRole.UserRole, section)
                    self.library_list.addItem(item)
            
            if self.music_libraries:
                self.library_list.setCurrentRow(0)
            else:
                self.info_label.setText("⚠️ No music libraries found!")
                self.info_label.setStyleSheet("color: #ff6b6b;")
        except Exception as e:
            self.info_label.setText(f"Error loading libraries: {e}")
            self.info_label.setStyleSheet("color: #ff6b6b;")
    
    def update_info(self):
        current = self.library_list.currentItem()
        if current:
            library = current.data(Qt.ItemDataRole.UserRole)
            self.selected_library = library
            try:
                track_count = sum(len(album.tracks()) for album in library.albums())
                self.info_label.setText(f"Selected: {library.title} - ~{track_count} tracks")
                self.info_label.setStyleSheet("color: #51cf66; font-style: normal;")
            except:
                self.info_label.setText(f"Selected: {library.title}")
                self.info_label.setStyleSheet("color: #51cf66; font-style: normal;")
    
    def get_selected_library(self):
        return self.selected_library


# ============================================================================
# DUPLICATE HANDLER
# ============================================================================

class DuplicateTrackHandler:
    @staticmethod
    def normalize_title(title: str) -> str:
        title_lower = title.lower().strip()
        suffixes = [' (remastered', ' (remaster', ' (deluxe', ' (live)', ' (live']
        for suffix in suffixes:
            if suffix in title_lower:
                title_lower = title_lower.split(suffix)[0].strip()
        return title_lower
    
    @staticmethod
    def deduplicate_tracks(tracks: List, log_callback=None) -> List:
        if not tracks:
            return []
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
        unique = []
        duplicates = 0
        for group in groups.values():
            if len(group) > 1:
                duplicates += len(group) - 1
            unique.append(group[0])
        if log_callback and duplicates > 0:
            log_callback(f"Deduplication: removed {duplicates} duplicates")
        return unique


# ============================================================================
# FAST MATCHING THREAD
# ============================================================================

class FastMatchingThread(QThread):
    log_signal = pyqtSignal(str)
    progress_signal = pyqtSignal(int, int)
    match_found_signal = pyqtSignal(dict)
    status_signal = pyqtSignal(str)
    finished_signal = pyqtSignal(list)
    
    def __init__(self, plex_server, tidal_session, options, selected_items=None, current_library=None):
        super().__init__()
        self.plex_server = plex_server
        self.tidal_session = tidal_session
        self.options = options
        self.selected_items = selected_items
        self.current_library = current_library
        self.is_running = True
        self.matches = []
        self.loader = OptimizedLibraryLoader(plex_server)
        
    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_signal.emit(f"[{timestamp}] {message}")
        
    def run(self):
        try:
            self.log("Collecting tracks (optimized)...")
            all_tracks = []
            
            if self.selected_items:
                self.log(f"Processing {len(self.selected_items)} selected items...")
                
                playlists = [item for item in self.selected_items if item['type'] == 'playlist']
                albums = [item for item in self.selected_items if item['type'] == 'album']
                artists = [item for item in self.selected_items if item['type'] == 'artist']
                
                if playlists:
                    self.log(f"Loading {len(playlists)} playlists...")
                    for item in playlists:
                        if not self.is_running:
                            break
                        try:
                            tracks = list(item['object'].items())
                            all_tracks.extend(tracks)
                            self.log(f"  Loaded {len(tracks)} tracks from playlist")
                        except Exception as e:
                            self.log(f"Error loading playlist: {str(e)}")
                
                if albums:
                    self.log(f"Loading {len(albums)} albums...")
                    for item in albums:
                        if not self.is_running:
                            break
                        try:
                            tracks = list(item['object'].tracks())
                            all_tracks.extend(tracks)
                        except Exception as e:
                            self.log(f"Error loading album: {str(e)}")
                
                if artists:
                    self.log(f"Loading {len(artists)} artists...")
                    for item in artists:
                        if not self.is_running:
                            break
                        try:
                            tracks = list(item['object'].tracks())
                            all_tracks.extend(tracks)
                            self.log(f"  Loaded {len(tracks)} tracks from artist")
                        except Exception as e:
                            self.log(f"Error loading artist: {str(e)}")
                            
            else:
                if self.current_library:
                    music_section = self.current_library
                    self.log(f"Using library: {music_section.title}")
                else:
                    music_sections = [s for s in self.plex_server.library.sections() if s.type == 'artist']
                    if not music_sections:
                        self.log("No music library found!")
                        return
                    music_section = music_sections[0]
                    self.log(f"Found music library: {music_section.title}")
                
                self.log("Loading tracks from library (this may take a moment)...")
                filter_params = {
                    'filter_lossless': self.options.get('filter_lossless', False),
                    'selected_formats': self.options.get('selected_formats', [])
                }
                
                all_tracks = self.loader.get_all_tracks_fast(music_section, filter_params)
                self.log(f"Loaded {len(all_tracks)} tracks")
            
            if self.options.get('filter_lossless', False):
                before = len(all_tracks)
                all_tracks = AudioFormatHandler.filter_lossless(all_tracks)
                self.log(f"Lossless filter: {before} -> {len(all_tracks)} tracks")
            
            if self.options.get('selected_formats'):
                before = len(all_tracks)
                filtered = []
                codec_cache = {}
                
                for track in all_tracks:
                    track_key = id(track)
                    if track_key not in codec_cache:
                        codec_cache[track_key] = AudioFormatHandler.get_track_codec(track)
                    
                    if codec_cache[track_key] in self.options['selected_formats']:
                        filtered.append(track)
                
                all_tracks = filtered
                self.log(f"Format filter: {before} -> {len(all_tracks)} tracks")
            
            if self.options.get('deduplicate', True):
                all_tracks = DuplicateTrackHandler.deduplicate_tracks(all_tracks, self.log)
            
            total = len(all_tracks)
            self.log(f"Processing {total} tracks...")
            
            artist_cache = {}
            matches = []
            
            for idx, track in enumerate(all_tracks):
                if not self.is_running:
                    break
                
                self.progress_signal.emit(idx + 1, total)
                
                try:
                    track_id = id(track)
                    if track_id not in artist_cache:
                        artist_cache[track_id] = track.artist().title if track.artist() else "Unknown"
                except:
                    continue
                
                if idx % 50 == 0:
                    self.log(f"Processing {idx + 1}/{total}: {track.title}")
                
                tidal_match = self.search_tidal_fast(track)
                
                if tidal_match:
                    popularity = getattr(tidal_match, 'popularity', 0)
                    match_info = {
                        'plex_track': track,
                        'tidal_track': tidal_match,
                        'popularity': popularity if popularity else 0,
                        'match_score': self.calculate_score(track, tidal_match)
                    }
                    matches.append(match_info)
                    self.match_found_signal.emit(match_info)
                    
                    if self.options.get('update_ratings'):
                        try:
                            track.rate(min(10, max(0, popularity / 10)))
                        except:
                            pass
                    
                    time.sleep(0.01)
            
            matches.sort(key=lambda x: x['popularity'], reverse=True)
            self.log(f"Completed! Found {len(matches)} matches")
            self.finished_signal.emit(matches)
            
        except Exception as e:
            self.log(f"Error: {e}")
            import traceback
            self.log(traceback.format_exc())
    
    def search_tidal_fast(self, plex_track):
        try:
            artist = plex_track.artist().title if plex_track.artist() else ""
            query = f"{artist} {plex_track.title}"[:100]
            
            results = self.tidal_session.search(query, models=[tidalapi.Track], limit=1)
            if results and 'tracks' in results and results['tracks']:
                return results['tracks'][0]
        except:
            pass
        return None
    
    def calculate_score(self, plex_track, tidal_track):
        score = 0
        try:
            if plex_track.title.lower() == tidal_track.name.lower():
                score += 50
            if plex_track.artist():
                if plex_track.artist().title.lower() == tidal_track.artist.name.lower():
                    score += 50
        except:
            pass
        return score
    
    def stop(self):
        self.is_running = False


# ============================================================================
# TIDAL LOGIN THREAD
# ============================================================================

class TidalLoginThread(QThread):
    login_success = pyqtSignal(object)
    login_error = pyqtSignal(str)
    log_signal = pyqtSignal(str)
    auth_url_signal = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        self.session = None
        
    def run(self):
        try:
            self.log_signal.emit("Checking for saved Tidal session...")
            self.session = tidalapi.Session()
            if self.load_session():
                if self.session.check_login():
                    self.log_signal.emit("✓ Using saved Tidal session!")
                    self.login_success.emit(self.session)
                    return
            
            self.log_signal.emit("Starting OAuth flow...")
            login, future = self.session.login_oauth()
            auth_url = login.verification_uri_complete
            self.auth_url_signal.emit(auth_url)
            self.log_signal.emit("Waiting for authentication...")
            
            for i in range(120):
                if future.done():
                    break
                time.sleep(1)
            
            if future.done():
                future.result()
                if self.session.check_login():
                    self.log_signal.emit("Login successful!")
                    self.save_session()
                    self.login_success.emit(self.session)
                else:
                    self.login_error.emit("Login failed")
            else:
                self.login_error.emit("Login timeout")
        except Exception as e:
            self.login_error.emit(str(e))
    
    def load_session(self):
        try:
            if os.path.exists(TIDAL_SESSION_FILE):
                with open(TIDAL_SESSION_FILE, 'r') as f:
                    data = json.load(f)
                    if data.get('expiry_time', 0) > time.time():
                        self.session.token_type = data.get('token_type', 'Bearer')
                        self.session.access_token = data.get('access_token')
                        self.session.refresh_token = data.get('refresh_token')
                        self.session.expiry_time = data.get('expiry_time')
                        return True
                    elif data.get('refresh_token'):
                        try:
                            self.session.token_refresh(data['refresh_token'])
                            self.save_session()
                            return True
                        except:
                            pass
        except:
            pass
        return False
    
    def save_session(self):
        try:
            if self.session and self.session.access_token:
                data = {
                    'token_type': getattr(self.session, 'token_type', 'Bearer'),
                    'access_token': self.session.access_token,
                    'refresh_token': getattr(self.session, 'refresh_token', None),
                    'expiry_time': getattr(self.session, 'expiry_time', 0)
                }
                with open(TIDAL_SESSION_FILE, 'w') as f:
                    json.dump(data, f)
                self.log_signal.emit("✓ Session saved for next time")
        except Exception as e:
            self.log_signal.emit(f"Error saving session: {e}")


# ============================================================================
# AUTH DIALOG
# ============================================================================

class AuthDialog(QDialog):
    def __init__(self, auth_url, parent=None):
        super().__init__(parent)
        self.auth_url = auth_url
        self.setWindowTitle("Tidal Authentication")
        self.setMinimumWidth(600)
        self.setMinimumHeight(200)
        
        layout = QVBoxLayoutDialog()
        layout.addWidget(QLabel("Open this URL in your browser to authenticate:"))
        
        url_label = QLabel(f'<a href="{self.auth_url}">{self.auth_url}</a>')
        url_label.setOpenExternalLinks(True)
        url_label.setStyleSheet("color: #00a8ff; padding: 10px;")
        layout.addWidget(url_label)
        
        copy_btn = QPushButton("Copy URL")
        copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(self.auth_url))
        layout.addWidget(copy_btn)
        
        open_btn = QPushButton("Open in Browser")
        open_btn.clicked.connect(lambda: webbrowser.open(self.auth_url))
        layout.addWidget(open_btn)
        
        continue_btn = QPushButton("I've Authenticated - Continue")
        continue_btn.clicked.connect(self.accept)
        continue_btn.setStyleSheet("background-color: #00a8ff;")
        layout.addWidget(continue_btn)
        
        self.setLayout(layout)


# ============================================================================
# MAIN GUI
# ============================================================================

class PlexTidalMatcherGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.matches = []
        self.tidal_session = None
        self.plex_server = None
        self.worker = None
        self.login_thread = None
        self.selected_items = []
        self.current_library = None
        self.music_libraries = []
        self.current_theme = ThemeManager.load_theme_preference()
        self.format_filter_active = False
        self.selected_formats = []
        self.lossless_only = False
        
        self.load_credentials()
        self.load_library_preference()
        self.init_ui()
        self.apply_theme()
        
        if self.auto_connect_check.isChecked() and self._saved_plex_token:
            QTimer.singleShot(500, self.auto_connect)
        
        QTimer.singleShot(1000, self.auto_connect_tidal)
    
    def load_credentials(self):
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
    
    def load_library_preference(self):
        self._saved_library_title = None
        try:
            if os.path.exists(LIBRARY_FILE):
                with open(LIBRARY_FILE, 'r') as f:
                    data = json.load(f)
                    self._saved_library_title = data.get('library_title')
        except:
            pass
    
    def save_library_preference(self):
        if self.current_library:
            try:
                with open(LIBRARY_FILE, 'w') as f:
                    json.dump({'library_title': self.current_library.title}, f)
            except:
                pass
    
    def save_credentials(self):
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
    
    def apply_theme(self):
        self.setStyleSheet(ThemeManager.get_theme(self.current_theme))
    
    def switch_theme(self, theme_name):
        self.current_theme = theme_name
        self.apply_theme()
        ThemeManager.save_theme_preference(theme_name)
        self.log(f"✓ Switched to {theme_name} theme")
    
    def init_ui(self):
        self.setWindowTitle("🎵 Plex Tidal Music Matcher (Optimized)")
        self.setGeometry(100, 100, 1200, 950)
        
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
        
        theme_menu = menubar.addMenu("Theme")
        for theme_name in ThemeManager.get_themes():
            theme_action = QAction(f"{theme_name.capitalize()} Theme", self)
            theme_action.triggered.connect(lambda checked, t=theme_name: self.switch_theme(t))
            theme_menu.addAction(theme_action)
        
        tools_menu = menubar.addMenu("Tools")
        stats_action = QAction("📊 Statistics Dashboard", self)
        stats_action.triggered.connect(self.show_statistics)
        tools_menu.addAction(stats_action)
        playlists_action = QAction("🎵 Create Smart Playlists", self)
        playlists_action.triggered.connect(self.show_playlist_creator)
        tools_menu.addAction(playlists_action)
        tools_menu.addSeparator()
        progress_action = QAction("📁 Manage Saved Progress", self)
        progress_action.triggered.connect(self.show_progress_manager)
        tools_menu.addAction(progress_action)
        tools_menu.addSeparator()
        clear_filter_action = QAction("Clear Format Filter", self)
        clear_filter_action.triggered.connect(self.clear_format_filter)
        tools_menu.addAction(clear_filter_action)
        
        account_menu = menubar.addMenu("Account")
        tidal_info_action = QAction("🌊 Tidal Account Info", self)
        tidal_info_action.triggered.connect(self.show_tidal_account_info)
        account_menu.addAction(tidal_info_action)
        account_menu.addSeparator()
        logout_tidal_action = QAction("🚪 Logout from Tidal", self)
        logout_tidal_action.triggered.connect(self.logout_tidal)
        account_menu.addAction(logout_tidal_action)
        
        view_menu = menubar.addMenu("View")
        clear_log_action = QAction("Clear Log", self)
        clear_log_action.triggered.connect(lambda: self.log_output.clear())
        view_menu.addAction(clear_log_action)
        
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        
        conn_group = QGroupBox("Connection Settings")
        conn_layout = QGridLayout()
        
        conn_layout.addWidget(QLabel("Plex URL:"), 0, 0)
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
        
        self.auto_connect_check = QCheckBox("Auto-connect")
        self.auto_connect_check.setChecked(True)
        plex_btn_layout.addWidget(self.auto_connect_check)
        conn_layout.addLayout(plex_btn_layout, 2, 1)
        
        self.tidal_btn = QPushButton("Login to Tidal")
        self.tidal_btn.clicked.connect(self.connect_tidal)
        conn_layout.addWidget(self.tidal_btn, 3, 1)
        
        self.status_label = QLabel("Not connected")
        self.status_label.setStyleSheet("color: #ff6b6b; padding: 5px;")
        conn_layout.addWidget(self.status_label, 4, 0, 1, 3)
        
        conn_group.setLayout(conn_layout)
        main_layout.addWidget(conn_group)
        
        library_group = QGroupBox("Library Information")
        library_layout = QHBoxLayout()
        
        self.library_label = QLabel("📚 No library selected")
        self.library_label.setStyleSheet("color: #888888; font-style: italic; padding: 5px;")
        library_layout.addWidget(self.library_label)
        library_layout.addStretch()
        
        self.library_select_btn = QPushButton("Change Library")
        self.library_select_btn.clicked.connect(self.select_library)
        self.library_select_btn.setEnabled(False)
        library_layout.addWidget(self.library_select_btn)
        
        library_group.setLayout(library_layout)
        main_layout.addWidget(library_group)
        
        selection_group = QGroupBox("Library Selection")
        selection_layout = QHBoxLayout()
        
        self.select_btn = QPushButton("🎵 Select Playlists/Albums/Artists")
        self.select_btn.clicked.connect(self.select_library_items)
        self.select_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold; padding: 12px;")
        self.select_btn.setEnabled(False)
        selection_layout.addWidget(self.select_btn)
        
        self.selection_status = QLabel("No items selected - will process entire library")
        self.selection_status.setStyleSheet("color: #888888; font-style: italic; padding: 5px;")
        selection_layout.addWidget(self.selection_status)
        
        self.clear_selection_btn = QPushButton("Clear Selection")
        self.clear_selection_btn.clicked.connect(self.clear_selection)
        self.clear_selection_btn.setEnabled(False)
        selection_layout.addWidget(self.clear_selection_btn)
        
        selection_layout.addStretch()
        selection_group.setLayout(selection_layout)
        main_layout.addWidget(selection_group)
        
        options_group = QGroupBox("Options")
        options_layout = QGridLayout()
        
        options_layout.addWidget(QLabel("Match Threshold (%):"), 0, 0)
        self.threshold_spin = QSpinBox()
        self.threshold_spin.setRange(50, 100)
        self.threshold_spin.setValue(70)
        options_layout.addWidget(self.threshold_spin, 0, 1)
        
        self.deduplicate_check = QCheckBox("Deduplicate tracks")
        self.deduplicate_check.setChecked(True)
        options_layout.addWidget(self.deduplicate_check, 1, 0, 1, 2)
        
        self.update_ratings_check = QCheckBox("Auto-update ratings")
        self.update_ratings_check.setChecked(False)
        options_layout.addWidget(self.update_ratings_check, 2, 0, 1, 2)
        
        self.filter_format_btn = QPushButton("🎵 Audio Format Filter")
        self.filter_format_btn.clicked.connect(self.show_format_filter)
        options_layout.addWidget(self.filter_format_btn, 3, 0, 1, 2)
        
        self.format_filter_status = QLabel("No format filter active")
        self.format_filter_status.setStyleSheet("color: #888888; font-style: italic; font-size: 11px;")
        options_layout.addWidget(self.format_filter_status, 4, 0, 1, 2)
        
        options_group.setLayout(options_layout)
        main_layout.addWidget(options_group)
        
        btn_layout = QHBoxLayout()
        
        self.start_btn = QPushButton("▶ Start Matching")
        self.start_btn.clicked.connect(self.start_matching)
        self.start_btn.setEnabled(False)
        self.start_btn.setStyleSheet("background-color: #00a8ff; font-weight: bold;")
        btn_layout.addWidget(self.start_btn)
        
        self.stop_btn = QPushButton("⬛ Stop")
        self.stop_btn.clicked.connect(self.stop_matching)
        self.stop_btn.setEnabled(False)
        btn_layout.addWidget(self.stop_btn)
        
        main_layout.addLayout(btn_layout)
        
        self.progress_bar = QProgressBar()
        main_layout.addWidget(self.progress_bar)
        
        self.progress_label = QLabel("Ready")
        main_layout.addWidget(self.progress_label)
        
        tabs = QTabWidget()
        
        results_widget = QWidget()
        results_layout = QVBoxLayout(results_widget)
        self.results_table = QTableWidget()
        self.results_table.setColumnCount(5)
        self.results_table.setHorizontalHeaderLabels(["Artist", "Track", "Album", "Popularity", "Rating"])
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
        tabs.addTab(log_widget, "Log")
        
        main_layout.addWidget(tabs)
        
        self.log("🎵 Plex Tidal Music Matcher (Optimized) initialized")
        self.setup_shortcuts()
    
    def setup_shortcuts(self):
        QShortcut(QKeySequence.StandardKey.Save, self).activated.connect(self.save_credentials)
        QShortcut(QKeySequence("F5"), self).activated.connect(self.start_matching)
        QShortcut(QKeySequence.StandardKey.Cancel, self).activated.connect(self.stop_matching)
        QShortcut(QKeySequence("Ctrl+E"), self).activated.connect(self.export_results)
    
    def log(self, message):
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_output.append(f"[{timestamp}] {message}")
        cursor = self.log_output.textCursor()
        cursor.movePosition(QTextCursor.MoveOperation.End)
        self.log_output.setTextCursor(cursor)
        QApplication.processEvents()
    
    def show_token_help(self):
        QMessageBox.information(self, "Plex Token Help",
            "To find your Plex token:\n\n"
            "1. Open Plex Web App\n"
            "2. Click on any media item\n"
            "3. Click ... → Get Info → View XML\n"
            "4. Look for 'X-Plex-Token=' in the URL")
    
    def auto_connect(self):
        if self._saved_plex_token:
            self.connect_plex()
    
    def auto_connect_tidal(self):
        if os.path.exists(TIDAL_SESSION_FILE):
            self.log("Found saved Tidal session, auto-connecting...")
            self.connect_tidal()
    
    def connect_plex(self):
        try:
            url = self.plex_url_input.text().strip()
            token = self.plex_token_input.text().strip()
            
            if not url or not token:
                QMessageBox.warning(self, "Missing Info", "Please enter Plex URL and Token")
                return
            
            self.plex_server = PlexServer(url, token)
            self.log(f"✓ Connected to Plex: {self.plex_server.friendlyName}")
            
            self.music_libraries = [s for s in self.plex_server.library.sections() if s.type == 'artist']
            
            if not self.music_libraries:
                self.log("⚠️ No music libraries found!")
                QMessageBox.warning(self, "No Music Library", 
                    "No music libraries found on this Plex server.\n"
                    "Please make sure you have a music library set up.")
                return
            
            if self._saved_library_title:
                for lib in self.music_libraries:
                    if lib.title == self._saved_library_title:
                        self.current_library = lib
                        self.log(f"✓ Restored library: {lib.title}")
                        break
            
            if not self.current_library:
                self.current_library = self.music_libraries[0]
                self.log(f"✓ Using library: {self.current_library.title}")
            
            self.library_label.setText(f"📚 Library: {self.current_library.title}")
            self.save_library_preference()
            
            self.save_credentials()
            self.plex_btn.setEnabled(False)
            self.select_btn.setEnabled(True)
            self.library_select_btn.setEnabled(True)
            self.update_status()
            
        except Exception as e:
            self.log(f"✗ Plex connection failed: {e}")
            QMessageBox.critical(self, "Connection Error", str(e))
    
    def select_library(self):
        if not self.plex_server or not self.music_libraries:
            return
        
        dialog = LibrarySelector(self.plex_server, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.current_library = dialog.get_selected_library()
            if self.current_library:
                self.log(f"✓ Selected library: {self.current_library.title}")
                self.library_label.setText(f"📚 Library: {self.current_library.title}")
                self.save_library_preference()
                
                self.selected_items = []
                self.selection_status.setText("No items selected - will process entire library")
                self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
                self.clear_selection_btn.setEnabled(False)
    
    def connect_tidal(self):
        try:
            self.tidal_btn.setEnabled(False)
            self.login_thread = TidalLoginThread()
            self.login_thread.log_signal.connect(self.log)
            self.login_thread.auth_url_signal.connect(self.show_auth_dialog)
            self.login_thread.login_success.connect(self.on_tidal_success)
            self.login_thread.login_error.connect(self.on_tidal_error)
            self.login_thread.start()
        except Exception as e:
            self.log(f"✗ Tidal error: {e}")
            self.tidal_btn.setEnabled(True)
    
    def show_auth_dialog(self, auth_url):
        dialog = AuthDialog(auth_url, self)
        dialog.exec()
    
    def on_tidal_success(self, session):
        self.tidal_session = session
        self.log(f"✓ Logged in as: {session.user.username}")
        self.tidal_btn.setEnabled(False)
        self.update_status()
    
    def on_tidal_error(self, error_msg):
        self.log(f"✗ Login failed: {error_msg}")
        self.tidal_btn.setEnabled(True)
    
    def logout_tidal(self):
        try:
            if os.path.exists(TIDAL_SESSION_FILE):
                os.remove(TIDAL_SESSION_FILE)
            self.tidal_session = None
            self.tidal_btn.setEnabled(True)
            self.log("✓ Logged out of Tidal (session cleared)")
            self.update_status()
        except Exception as e:
            self.log(f"✗ Error: {e}")
    
    def update_status(self):
        if self.plex_server and self.tidal_session:
            self.status_label.setText("✓ Connected - Ready!")
            self.status_label.setStyleSheet("color: #51cf66;")
            self.start_btn.setEnabled(True)
        elif self.plex_server:
            self.status_label.setText("✓ Plex connected - Login to Tidal")
            self.status_label.setStyleSheet("color: #ffa500;")
        elif self.tidal_session:
            self.status_label.setText("✓ Tidal connected - Connect to Plex")
            self.status_label.setStyleSheet("color: #ffa500;")
    
    def select_library_items(self):
        if not self.plex_server:
            QMessageBox.warning(self, "Not Connected", "Please connect to Plex first")
            return
        
        if not self.current_library:
            QMessageBox.warning(self, "No Library", "Please select a music library first")
            return
        
        dialog = FastLibrarySelectorDialog(self.plex_server, self, self.current_library)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.selected_items = dialog.get_selected_items()
            
            if self.selected_items:
                type_counts = {}
                total_tracks = 0
                for item in self.selected_items:
                    item_type = item['type']
                    type_counts[item_type] = type_counts.get(item_type, 0) + 1
                    
                status_parts = [f"{count} {item_type}{'s' if count > 1 else ''}" 
                               for item_type, count in type_counts.items()]
                self.selection_status.setText(f"Selected: {', '.join(status_parts)}")
                self.selection_status.setStyleSheet("color: #51cf66; font-style: normal; font-weight: bold;")
                self.clear_selection_btn.setEnabled(True)
                self.log(f"Selected {len(self.selected_items)} items: {', '.join(status_parts)}")
            else:
                self.selection_status.setText("No items selected - will process entire library")
                self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
                self.clear_selection_btn.setEnabled(False)
    
    def clear_selection(self):
        self.selected_items = []
        self.selection_status.setText("No items selected - will process entire library")
        self.selection_status.setStyleSheet("color: #888888; font-style: italic;")
        self.clear_selection_btn.setEnabled(False)
        self.log("Selection cleared - will process entire library")
    
    def show_format_filter(self):
        dialog = FormatFilterDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.lossless_only = dialog.is_lossless_only()
            self.selected_formats = dialog.get_selected_formats()
            self.format_filter_active = self.lossless_only or bool(self.selected_formats)
            
            if self.format_filter_active:
                if self.lossless_only:
                    self.format_filter_status.setText("🎵 Filter: Lossless only (ALAC, FLAC, WAV, etc.)")
                elif self.selected_formats:
                    formats = ', '.join(f.upper() for f in self.selected_formats[:3])
                    if len(self.selected_formats) > 3:
                        formats += f" +{len(self.selected_formats)-3} more"
                    self.format_filter_status.setText(f"🎵 Filter: {formats}")
                self.format_filter_status.setStyleSheet("color: #51cf66; font-style: normal; font-size: 11px;")
            else:
                self.format_filter_status.setText("No format filter active")
                self.format_filter_status.setStyleSheet("color: #888888; font-style: italic; font-size: 11px;")
    
    def clear_format_filter(self):
        self.format_filter_active = False
        self.selected_formats = []
        self.lossless_only = False
        self.format_filter_status.setText("No format filter active")
        self.format_filter_status.setStyleSheet("color: #888888; font-style: italic; font-size: 11px;")
        self.log("✓ Format filter cleared")
    
    def show_statistics(self):
        if not self.matches:
            QMessageBox.information(self, "No Data", "Run matching first")
            return
        dialog = StatsDialog(self.matches, self)
        dialog.exec()
    
    def show_playlist_creator(self):
        if not self.plex_server:
            QMessageBox.warning(self, "Not Connected", "Connect to Plex first")
            return
        if not self.matches:
            QMessageBox.information(self, "No Matches", "Run matching first")
            return
        dialog = PlaylistCreationDialog(self.plex_server, self.matches, self)
        dialog.exec()
    
    def show_progress_manager(self):
        dialog = ProgressDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            progress = ProgressManager.load_progress()
            if progress:
                self.results_table.setRowCount(0)
                for match_data in progress.get('matches', []):
                    row = self.results_table.rowCount()
                    self.results_table.insertRow(row)
                    self.results_table.setItem(row, 0, QTableWidgetItem(match_data.get('artist', '')))
                    self.results_table.setItem(row, 1, QTableWidgetItem(match_data.get('track', '')))
                    self.results_table.setItem(row, 2, QTableWidgetItem(match_data.get('album', '')))
                    self.results_table.setItem(row, 3, QTableWidgetItem(str(match_data.get('popularity', 0))))
                    self.results_table.setItem(row, 4, QTableWidgetItem(f"{match_data.get('rating', 0):.1f} ★"))
                self.log(f"✓ Loaded {progress.get('matches_count', 0)} matches")
    
    def show_tidal_account_info(self):
        if not self.tidal_session:
            QMessageBox.warning(self, "Not Connected", "Login to Tidal first")
            return
        dialog = TidalAccountDialog(self.tidal_session, self)
        dialog.exec()
    
    def show_results_context_menu(self, position):
        menu = QMenu()
        copy_action = QAction("Copy Track Info", self)
        copy_action.triggered.connect(self.copy_selected_track_info)
        menu.addAction(copy_action)
        menu.exec(self.results_table.viewport().mapToGlobal(position))
    
    def copy_selected_track_info(self):
        row = self.results_table.currentRow()
        if row >= 0:
            artist = self.results_table.item(row, 0).text()
            track = self.results_table.item(row, 1).text()
            QApplication.clipboard().setText(f"{artist} - {track}")
            self.log(f"Copied: {artist} - {track}")
    
    def start_matching(self):
        if not self.plex_server or not self.tidal_session:
            return
        
        options = {
            'match_threshold': self.threshold_spin.value(),
            'deduplicate': self.deduplicate_check.isChecked(),
            'update_ratings': self.update_ratings_check.isChecked(),
            'limit_artists': False,
            'filter_lossless': self.lossless_only,
            'selected_formats': self.selected_formats if self.format_filter_active else []
        }
        
        self.matches = []
        self.results_table.setRowCount(0)
        
        self.worker = FastMatchingThread(
            self.plex_server, 
            self.tidal_session, 
            options, 
            self.selected_items or None,
            self.current_library
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
        self.clear_selection_btn.setEnabled(False)
        self.library_select_btn.setEnabled(False)
    
    def stop_matching(self):
        if self.worker:
            self.worker.stop()
    
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
    
    def matching_finished(self, matches):
        self.log(f"✓ Completed! Found {len(matches)} matches")
        self.progress_label.setText(f"Complete - {len(matches)} matches")
        
        if matches:
            ProgressManager.save_progress(matches)
            self.log("✓ Progress saved")
        
        self.start_btn.setEnabled(True)
        self.stop_btn.setEnabled(False)
        self.select_btn.setEnabled(True)
        self.library_select_btn.setEnabled(True)
        
        if self.selected_items:
            self.clear_selection_btn.setEnabled(True)
    
    def export_results(self):
        if not self.matches:
            QMessageBox.information(self, "No Results", "No matches to export")
            return
        
        filename, _ = QFileDialog.getSaveFileName(
            self, "Save Results",
            f"plex_tidal_matches_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "JSON Files (*.json)"
        )
        
        if filename:
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
                    'rating': min(5, max(0, match['popularity'] / 20))
                })
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(export_data, f, indent=2)
            
            self.log(f"✓ Exported to {filename}")
    
    def closeEvent(self, event):
        if self.plex_token_input.text():
            self.save_credentials()
        event.accept()


def main():
    print("Starting Plex Tidal Matcher (Optimized)...")
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = PlexTidalMatcherGUI()
    window.show()
    print("Window displayed.")
    return app.exec()


if __name__ == "__main__":
    sys.exit(main())