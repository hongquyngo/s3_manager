# app.py - AWS S3 File Manager for Streamlit

import streamlit as st
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
import os
from datetime import datetime
import mimetypes
from pathlib import Path
import pandas as pd
from io import BytesIO
import logging

# Import your existing modules
from utils.auth import AuthManager
from utils.config import config
from utils.db import get_db_engine
from utils.s3_advanced import S3AdvancedManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="AWS S3 File Manager",
    page_icon="📁",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    /* Improve button styling */
    .stButton > button {
        height: 36px;
        padding: 0 16px;
    }
    
    /* Folder navigation buttons */
    .stButton > button[kind="secondary"] {
        height: 80px;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border: 1px solid #ddd;
    }
    
    .stButton > button[kind="secondary"]:hover {
        background-color: #e0e2e6;
        border-color: #0066cc;
    }
    
    /* Improve dataframe styling */
    .dataframe {
        font-size: 14px;
    }
    
    /* Improve sidebar styling */
    .css-1d391kg {
        padding-top: 1rem;
    }
    
    /* Better file/folder icons */
    .stDataFrame [data-testid="StyledLinkIconContainer"] {
        display: none;
    }
</style>
""", unsafe_allow_html=True)

# Initialize auth manager
auth_manager = AuthManager()

# S3 Manager Class
class S3Manager:
    def __init__(self):
        aws_config = config.get_aws_config()
        self.bucket_name = aws_config.get('bucket_name')
        self.app_prefix = aws_config.get('app_prefix', 'streamlit-app')
        self.region = aws_config.get('region', 'ap-southeast-1')
        
        # Initialize S3 client
        try:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_config.get('access_key_id'),
                aws_secret_access_key=aws_config.get('secret_access_key'),
                region_name=self.region
            )
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            self.connected = True
            
            # Initialize advanced manager
            self.advanced = S3AdvancedManager(
                self.s3_client, 
                self.bucket_name, 
                self.app_prefix
            )
        except Exception as e:
            logger.error(f"Failed to connect to S3: {e}")
            self.connected = False
            self.s3_client = None
            self.advanced = None
    
    def format_path(self, path):
        """Format path to ensure it starts with app_prefix"""
        if not path:
            return self.app_prefix + "/"
        
        # Remove leading/trailing slashes
        path = path.strip("/")
        
        # If path doesn't start with app_prefix, add it
        if not path.startswith(self.app_prefix):
            path = f"{self.app_prefix}/{path}"
        
        # Ensure path ends with / for folders
        if not path.endswith("/"):
            path += "/"
        
        return path
    
    def list_objects(self, prefix=""):
        """List objects in S3 bucket with given prefix"""
        if not self.connected:
            return [], []
        
        try:
            # Format the prefix
            if prefix:
                full_prefix = self.format_path(prefix)
            else:
                full_prefix = self.app_prefix + "/"
            
            # List objects
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=full_prefix,
                Delimiter='/'
            )
            
            # Get folders (common prefixes)
            folders = []
            if 'CommonPrefixes' in response:
                for prefix in response['CommonPrefixes']:
                    folder_path = prefix['Prefix']
                    folder_name = folder_path.rstrip('/').split('/')[-1]
                    folders.append({
                        'name': folder_name,
                        'path': folder_path,
                        'type': 'folder',
                        'size': '-',
                        'modified': '-'
                    })
            
            # Get files
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    # Skip the folder itself
                    if obj['Key'] == full_prefix:
                        continue
                    
                    file_path = obj['Key']
                    file_name = file_path.split('/')[-1]
                    
                    # Skip if it's a folder marker
                    if not file_name:
                        continue
                    
                    files.append({
                        'name': file_name,
                        'path': file_path,
                        'type': 'file',
                        'size': self.format_size(obj['Size']),
                        'size_bytes': obj['Size'],
                        'modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                    })
            
            return folders, files
            
        except Exception as e:
            st.error(f"Error listing objects: {e}")
            return [], []
    
    def upload_file(self, file, path=""):
        """Upload file to S3"""
        if not self.connected:
            return False, "Not connected to S3"
        
        try:
            # Format the path
            full_path = self.format_path(path)
            file_key = full_path + file.name
            
            # Upload file
            self.s3_client.upload_fileobj(
                file,
                self.bucket_name,
                file_key
            )
            
            return True, f"File '{file.name}' uploaded successfully"
            
        except Exception as e:
            logger.error(f"Upload error: {e}")
            return False, f"Upload failed: {str(e)}"
    
    def download_file(self, file_path):
        """Download file from S3"""
        if not self.connected:
            return None, "Not connected to S3"
        
        try:
            # Get file content
            response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            
            content = response['Body'].read()
            return content, None
            
        except Exception as e:
            logger.error(f"Download error: {e}")
            return None, f"Download failed: {str(e)}"
    
    def delete_object(self, object_path):
        """Delete file or folder from S3"""
        if not self.connected:
            return False, "Not connected to S3"
        
        try:
            # Check if it's a folder (ends with /)
            if object_path.endswith('/'):
                # List all objects in folder
                objects = self.s3_client.list_objects_v2(
                    Bucket=self.bucket_name,
                    Prefix=object_path
                )
                
                # Delete all objects in folder
                if 'Contents' in objects:
                    delete_objects = [{'Key': obj['Key']} for obj in objects['Contents']]
                    
                    if delete_objects:
                        self.s3_client.delete_objects(
                            Bucket=self.bucket_name,
                            Delete={'Objects': delete_objects}
                        )
                
                return True, "Folder deleted successfully"
            else:
                # Delete single file
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=object_path
                )
                return True, "File deleted successfully"
                
        except Exception as e:
            logger.error(f"Delete error: {e}")
            return False, f"Delete failed: {str(e)}"
    
    def create_folder(self, folder_name, parent_path=""):
        """Create a folder in S3"""
        if not self.connected:
            return False, "Not connected to S3"
        
        try:
            # Format the path
            if parent_path:
                full_path = self.format_path(parent_path)
            else:
                full_path = self.app_prefix + "/"
            
            folder_key = full_path + folder_name + "/"
            
            # Create empty object as folder
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=folder_key,
                Body=b''
            )
            
            return True, f"Folder '{folder_name}' created successfully"
            
        except Exception as e:
            logger.error(f"Create folder error: {e}")
            return False, f"Create folder failed: {str(e)}"
    
    def format_size(self, size_bytes):
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

# Login page
def show_login_page():
    st.title("🔐 Login")
    
    col1, col2, col3 = st.columns([1, 2, 1])
    
    with col2:
        with st.form("login_form"):
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            submit = st.form_submit_button("Login", use_container_width=True)
            
            if submit:
                if username and password:
                    success, result = auth_manager.authenticate(username, password)
                    
                    if success:
                        auth_manager.login(result)
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error(result.get("error", "Login failed"))
                else:
                    st.error("Please enter both username and password")

# Main application
def show_main_app():
    # Initialize S3 manager
    if 's3_manager' not in st.session_state:
        st.session_state.s3_manager = S3Manager()
    
    s3_manager = st.session_state.s3_manager
    
    # Check S3 connection
    if not s3_manager.connected:
        st.error("❌ Failed to connect to AWS S3. Please check your configuration.")
        
        with st.expander("🔧 Troubleshooting Guide"):
            st.markdown("""
            **Common issues:**
            1. **Invalid AWS credentials**: Check your AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
            2. **Wrong bucket name**: Verify S3_BUCKET_NAME in your configuration
            3. **Missing permissions**: Ensure your IAM user has ListBucket, GetObject, PutObject, DeleteObject permissions
            4. **Network issues**: Check your internet connection
            
            **Configuration location:**
            - Local: Check your `.env` file
            - Streamlit Cloud: Check app secrets in Settings
            """)
        return
    
    # Sidebar
    with st.sidebar:
        st.title("AWS S3 File Manager")
        st.divider()
        
        # User info
        st.write(f"👤 **User:** {auth_manager.get_user_display_name()}")
        st.write(f"🪣 **Bucket:** {s3_manager.bucket_name}")
        st.write(f"📁 **Root:** /{s3_manager.app_prefix}")
        
        st.divider()
        
        # Navigation path
        if 'current_path' not in st.session_state:
            st.session_state.current_path = ""
        
        st.write("📍 **Current Path:**")
        display_path = st.session_state.current_path or "/"
        st.code(display_path)
        
        # Quick navigation
        with st.expander("🚀 Quick Navigation"):
            nav_path = st.text_input("Enter path to navigate", value=st.session_state.current_path)
            if st.button("Go", key="quick_nav"):
                st.session_state.current_path = nav_path
                st.rerun()
        
        # Actions
        st.divider()
        st.write("**Actions:**")
        
        # Create folder
        with st.expander("➕ Create Folder"):
            new_folder = st.text_input("Folder name")
            if st.button("Create", key="create_folder"):
                if new_folder:
                    success, message = s3_manager.create_folder(
                        new_folder, 
                        st.session_state.current_path
                    )
                    if success:
                        st.success(message)
                        st.rerun()
                    else:
                        st.error(message)
        
        # Upload files
        with st.expander("📤 Upload Files"):
            uploaded_files = st.file_uploader(
                "Choose files",
                accept_multiple_files=True,
                key="file_uploader"
            )
            
            if uploaded_files:
                if st.button("Upload", key="upload_btn"):
                    progress = st.progress(0)
                    total = len(uploaded_files)
                    
                    for i, file in enumerate(uploaded_files):
                        success, message = s3_manager.upload_file(
                            file,
                            st.session_state.current_path
                        )
                        if success:
                            st.success(f"✅ {file.name}")
                        else:
                            st.error(f"❌ {file.name}: {message}")
                        
                        progress.progress((i + 1) / total)
                    
                    st.rerun()
        
        # Search functionality
        with st.expander("🔍 Search Files"):
            search_term = st.text_input("Search for files/folders", key="search_input")
            if st.button("Search", key="search_btn") and search_term:
                with st.spinner("Searching..."):
                    results = s3_manager.advanced.search_files(
                        search_term, 
                        st.session_state.current_path
                    )
                    
                    if results:
                        st.success(f"Found {len(results)} results")
                        for result in results[:10]:  # Show max 10 results
                            col1, col2 = st.columns([3, 1])
                            with col1:
                                st.write(f"{'📁' if result['type'] == 'folder' else '📄'} {result['name']}")
                            with col2:
                                if st.button("Go to", key=f"goto_{result['path']}"):
                                    # Navigate to parent folder
                                    parent_path = '/'.join(result['path'].split('/')[:-1])
                                    if parent_path.startswith(s3_manager.app_prefix):
                                        parent_path = parent_path[len(s3_manager.app_prefix):].lstrip('/')
                                    st.session_state.current_path = parent_path
                                    st.rerun()
                    else:
                        st.info("No results found")
        
        st.divider()
        
        # Logout button
        if st.button("🚪 Logout", use_container_width=True):
            auth_manager.logout()
            st.rerun()
        
        # Version info
        st.divider()
        st.caption("AWS S3 File Manager v1.1.0")
        st.caption(f"© 2024 iSCM Dashboard")
    
    # Main content
    st.title("📁 File Manager")
    
    # Quick help
    with st.expander("ℹ️ Quick Help", expanded=False):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            **Navigation:**
            - Click folders to browse
            - Use breadcrumb to go back
            - Search files in sidebar
            - **NEW: Click folder cards to enter**
            """)
        with col2:
            st.markdown("""
            **File Operations:**
            - Select files with checkboxes
            - Upload via sidebar
            - Download single files
            """)
        with col3:
            st.markdown("""
            **Advanced:**
            - Rename files/folders
            - Preview supported files
            - Generate share links
            """)
    
    # Breadcrumb navigation
    if st.session_state.current_path:
        path_parts = st.session_state.current_path.strip("/").split("/")
        
        # Create breadcrumb
        breadcrumb_cols = st.columns(len(path_parts) + 1)
        
        # Home button
        with breadcrumb_cols[0]:
            if st.button("🏠 Home", key="home", use_container_width=True):
                st.session_state.current_path = ""
                st.rerun()
        
        # Path parts
        for i, part in enumerate(path_parts):
            if part and part != s3_manager.app_prefix:
                with breadcrumb_cols[i + 1]:
                    partial_path = "/".join(path_parts[:i+1])
                    if st.button(f"📁 {part}", key=f"path_{i}", use_container_width=True):
                        st.session_state.current_path = partial_path
                        st.rerun()
    else:
        # Just show home indicator when at root
        st.caption("📍 You are at the root folder")
    
    st.divider()
    
    # Quick upload area (drag & drop)
    upload_container = st.container()
    with upload_container:
        quick_upload = st.file_uploader(
            "📤 **Quick Upload** - Drag & drop files here or click to browse",
            accept_multiple_files=True,
            key="quick_upload",
            label_visibility="visible"
        )
        
        if quick_upload:
            if st.button("⬆️ Upload Files", key="quick_upload_btn"):
                progress = st.progress(0)
                success_count = 0
                
                for i, file in enumerate(quick_upload):
                    success, message = s3_manager.upload_file(
                        file,
                        st.session_state.current_path
                    )
                    if success:
                        success_count += 1
                    else:
                        st.error(f"❌ {file.name}: {message}")
                    
                    progress.progress((i + 1) / len(quick_upload))
                
                if success_count > 0:
                    st.success(f"✅ Uploaded {success_count}/{len(quick_upload)} files successfully!")
                    st.balloons()
                    st.rerun()
    
    st.divider()
    
    # List files and folders
    folders, files = s3_manager.list_objects(st.session_state.current_path)
    
    # Display count
    col1, col2, col3 = st.columns([2, 2, 6])
    with col1:
        st.write(f"📁 Folders: {len(folders)}")
    with col2:
        st.write(f"📄 Files: {len(files)}")
    with col3:
        if st.button("🔄 Refresh", key="refresh_list"):
            st.rerun()
    
    # Create dataframe for display
    all_items = folders + files
    
    if all_items:
        # Display folders as clickable cards first
        if folders:
            st.subheader("📁 Folders")
            st.caption("💡 Click on any folder below to open it")
            
            # Create a grid layout for folders
            folder_cols = st.columns(4)  # 4 columns
            
            for idx, folder in enumerate(folders):
                with folder_cols[idx % 4]:
                    # Create a button that looks like a folder card
                    if st.button(
                        f"📁\n**{folder['name']}**",
                        key=f"folder_nav_{idx}",
                        use_container_width=True,
                        help=f"Click to open {folder['name']}"
                    ):
                        # Navigate to folder
                        folder_path = folder['path'].rstrip('/')
                        if folder_path.startswith(s3_manager.app_prefix):
                            folder_path = folder_path[len(s3_manager.app_prefix):].lstrip('/')
                        
                        st.session_state.current_path = folder_path
                        st.rerun()
            
            st.divider()
        
        # Display files separately if any
        if files:
            with st.expander(f"📄 Files ({len(files)})", expanded=True):
                files_df = pd.DataFrame([{
                    'Type': '📄',
                    'Name': f['name'],
                    'Size': f['size'],
                    'Modified': f['modified']
                } for f in files])
                
                st.dataframe(files_df, use_container_width=True, hide_index=True)
        
        # Display all items in table for other operations
        with st.expander("🛠️ File Operations (Select items for actions)", expanded=True):
            st.caption("Select items using checkboxes to perform operations like Delete, Download, Rename, etc.")
        
        # Prepare data for display
        df_data = []
        for item in all_items:
            df_data.append({
                'Type': '📁' if item['type'] == 'folder' else '📄',
                'Name': item['name'],
                'Size': item['size'],
                'Modified': item['modified'],
                'Path': item['path']
            })
        
        df = pd.DataFrame(df_data)
        
        # Display table with selection
        selected = st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            on_select="rerun",
            selection_mode="multi-row"
        )
        
        # Handle selection
        if selected and selected.selection.rows:
            selected_indices = selected.selection.rows
            
            # Show selected items info
            if len(selected_indices) == 1:
                item = all_items[selected_indices[0]]
                st.info(f"Selected: {item['name']} ({item['type']})")
            else:
                st.info(f"Selected: {len(selected_indices)} items")
            
            col1, col2, col3, col4, col5, col6 = st.columns([1.5, 1.5, 1.2, 1.2, 1, 3.6])
            
            # Actions for selected items
            with col1:
                if st.button("🗑️ Delete Selected"):
                    if st.session_state.get('confirm_delete'):
                        for idx in selected_indices:
                            item = all_items[idx]
                            success, message = s3_manager.delete_object(item['path'])
                            if success:
                                st.success(f"Deleted: {item['name']}")
                            else:
                                st.error(f"Failed to delete {item['name']}: {message}")
                        
                        st.session_state.confirm_delete = False
                        st.rerun()
                    else:
                        st.session_state.confirm_delete = True
                        st.warning("Click again to confirm deletion")
            
            with col2:
                # Download button (only for single file selection)
                if len(selected_indices) == 1:
                    item = all_items[selected_indices[0]]
                    if item['type'] == 'file':
                        if st.button("⬇️ Download"):
                            content, error = s3_manager.download_file(item['path'])
                            if content:
                                st.download_button(
                                    label=f"Download {item['name']}",
                                    data=content,
                                    file_name=item['name'],
                                    key="download_file"
                                )
                            else:
                                st.error(error)
            
            with col3:
                # Additional actions for single selection
                if len(selected_indices) == 1:
                    item = all_items[selected_indices[0]]
                    
                    # Rename button
                    if st.button("✏️ Rename"):
                        st.session_state.renaming_item = item
            
            with col4:
                # Additional actions for single selection
                if len(selected_indices) == 1:
                    item = all_items[selected_indices[0]]
                    
                    # Preview button (files only)
                    if item['type'] == 'file' and st.button("👁️ Preview"):
                        st.session_state.preview_item = item
            
            with col5:
                # Additional actions for single selection
                if len(selected_indices) == 1:
                    item = all_items[selected_indices[0]]
                    
                    # Share link button (files only)
                    if item['type'] == 'file' and st.button("🔗 Share"):
                        st.session_state.share_item = item
            
            with col6:
                # Additional actions for single selection
                if len(selected_indices) == 1:
                    item = all_items[selected_indices[0]]
                    
                    # Info button
                    if st.button("ℹ️ Info"):
                        st.session_state.info_item = item
            
            # Add open folder button if a folder is selected
            if len(selected_indices) == 1:
                item = all_items[selected_indices[0]]
                if item['type'] == 'folder':
                    st.write("")  # Add some space
                    if st.button(f"📂 **Open folder: {item['name']}**", key="open_folder_main", use_container_width=True, type="primary"):
                        # Remove app_prefix from display path
                        folder_path = item['path'].rstrip('/')
                        if folder_path.startswith(s3_manager.app_prefix):
                            folder_path = folder_path[len(s3_manager.app_prefix):].lstrip('/')
                        
                        st.session_state.current_path = folder_path
                        st.rerun()
    else:
        # Empty folder state
        st.info("📭 This folder is empty")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown("""
            ### 📤 Upload Files
            Use the **Quick Upload** area above or the sidebar to add files to this folder.
            """)
        with col2:
            st.markdown("""
            ### 📁 Create Subfolder
            Use the **Create Folder** option in the sidebar to organize your files.
            """)
        with col3:
            st.markdown("""
            ### 🔙 Go Back
            Use the breadcrumb navigation or Home button to return to parent folder.
            """)
        
        # Show a big upload area for empty folders
        st.divider()
        empty_upload = st.file_uploader(
            "📤 **Drop files here to upload to this folder**",
            accept_multiple_files=True,
            key="empty_folder_upload",
            label_visibility="visible"
        )
        
        if empty_upload:
            if st.button("⬆️ Upload to this folder", key="empty_upload_btn", type="primary"):
                progress = st.progress(0)
                for i, file in enumerate(empty_upload):
                    success, message = s3_manager.upload_file(
                        file,
                        st.session_state.current_path
                    )
                    if success:
                        st.success(f"✅ Uploaded {file.name}")
                    else:
                        st.error(f"❌ {file.name}: {message}")
                    
                    progress.progress((i + 1) / len(empty_upload))
                
                st.balloons()
                st.rerun()
    
    # Modals/Dialogs for advanced features
    
    # Rename dialog
    if 'renaming_item' in st.session_state and st.session_state.renaming_item:
        item = st.session_state.renaming_item
        with st.container():
            st.divider()
            st.subheader(f"✏️ Rename {item['type']}: {item['name']}")
            new_name = st.text_input("New name", value=item['name'])
            col1, col2 = st.columns(2)
            with col1:
                if st.button("Rename", key="confirm_rename"):
                    if new_name and new_name != item['name']:
                        success, message = s3_manager.advanced.rename_object(
                            item['path'], 
                            new_name
                        )
                        if success:
                            st.success(message)
                            st.session_state.renaming_item = None
                            st.rerun()
                        else:
                            st.error(message)
            with col2:
                if st.button("Cancel", key="cancel_rename"):
                    st.session_state.renaming_item = None
                    st.rerun()
    
    # Preview dialog
    if 'preview_item' in st.session_state and st.session_state.preview_item:
        item = st.session_state.preview_item
        with st.container():
            st.divider()
            st.subheader(f"👁️ Preview: {item['name']}")
            
            with st.spinner("Loading preview..."):
                preview_data = s3_manager.advanced.get_file_preview(item['path'])
                
                if preview_data['type'] == 'text':
                    st.text_area(
                        "Content", 
                        value=preview_data['content'], 
                        height=400,
                        disabled=True
                    )
                    if preview_data.get('truncated'):
                        st.warning("File truncated for preview")
                
                elif preview_data['type'] == 'image':
                    st.image(preview_data['content'])
                
                elif preview_data['type'] == 'csv' or preview_data['type'] == 'excel':
                    st.write(f"Shape: {preview_data['shape']}")
                    st.dataframe(preview_data['content'])
                
                elif preview_data['type'] == 'error':
                    st.error(preview_data['message'])
                
                else:
                    st.info(f"Preview not available for {preview_data.get('extension', 'this file type')}")
            
            if st.button("Close Preview", key="close_preview"):
                st.session_state.preview_item = None
                st.rerun()
    
    # Share link dialog
    if 'share_item' in st.session_state and st.session_state.share_item:
        item = st.session_state.share_item
        with st.container():
            st.divider()
            st.subheader(f"🔗 Share Link: {item['name']}")
            
            expiration_hours = st.slider(
                "Link expiration (hours)", 
                min_value=1, 
                max_value=168,  # 1 week
                value=24
            )
            
            if st.button("Generate Link", key="generate_link"):
                url = s3_manager.advanced.generate_presigned_url(
                    item['path'], 
                    expiration=expiration_hours * 3600
                )
                if url:
                    st.success("Link generated!")
                    st.code(url)
                    st.info(f"This link will expire in {expiration_hours} hours")
                else:
                    st.error("Failed to generate link")
            
            if st.button("Close", key="close_share"):
                st.session_state.share_item = None
                st.rerun()
    
    # Info dialog
    if 'info_item' in st.session_state and st.session_state.info_item:
        item = st.session_state.info_item
        with st.container():
            st.divider()
            st.subheader(f"ℹ️ Information: {item['name']}")
            
            if item['type'] == 'folder':
                with st.spinner("Calculating folder size..."):
                    folder_info = s3_manager.advanced.get_folder_size(item['path'])
                    st.write(f"**Total Size:** {folder_info['formatted_size']}")
                    st.write(f"**File Count:** {folder_info['file_count']}")
                    if 'error' in folder_info:
                        st.error(folder_info['error'])
            else:
                with st.spinner("Loading metadata..."):
                    metadata = s3_manager.advanced.get_file_metadata(item['path'])
                    if 'error' not in metadata:
                        st.write(f"**Size:** {metadata['formatted_size']}")
                        st.write(f"**Type:** {metadata.get('mime_type', metadata['content_type'])}")
                        st.write(f"**Modified:** {metadata['last_modified']}")
                        st.write(f"**Storage Class:** {metadata['storage_class']}")
                        st.write(f"**ETag:** {metadata['etag']}")
                        
                        if metadata['metadata']:
                            st.write("**Custom Metadata:**")
                            for key, value in metadata['metadata'].items():
                                st.write(f"- {key}: {value}")
                    else:
                        st.error(metadata['error'])
            
            st.write(f"**Full Path:** `{item['path']}`")
            
            if st.button("Close", key="close_info"):
                st.session_state.info_item = None
                st.rerun()

# Main app logic
def main():
    # Initialize session state
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    # Check authentication
    if not auth_manager.check_session():
        show_login_page()
    else:
        show_main_app()

if __name__ == "__main__":
    main()