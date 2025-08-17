# utils/s3_advanced.py - Advanced features for S3 File Manager

import boto3
import logging
from datetime import datetime, timedelta
import mimetypes
from typing import Optional, Dict, List, Tuple
from PIL import Image
from io import BytesIO
import pandas as pd

logger = logging.getLogger(__name__)

class S3AdvancedManager:
    """Advanced S3 operations for the file manager"""
    
    def __init__(self, s3_client, bucket_name: str, app_prefix: str):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.app_prefix = app_prefix
    
    def rename_object(self, old_path: str, new_name: str) -> Tuple[bool, str]:
        """Rename a file or folder in S3"""
        try:
            # Determine if it's a folder
            is_folder = old_path.endswith('/')
            
            # Get parent directory
            path_parts = old_path.rstrip('/').split('/')
            parent_path = '/'.join(path_parts[:-1])
            
            # Create new path
            if parent_path:
                new_path = f"{parent_path}/{new_name}"
            else:
                new_path = new_name
            
            if is_folder:
                new_path += '/'
                
                # List all objects in the folder
                paginator = self.s3_client.get_paginator('list_objects_v2')
                pages = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=old_path
                )
                
                # Copy all objects to new location
                for page in pages:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            old_key = obj['Key']
                            new_key = old_key.replace(old_path, new_path, 1)
                            
                            # Copy object
                            self.s3_client.copy_object(
                                Bucket=self.bucket_name,
                                CopySource={'Bucket': self.bucket_name, 'Key': old_key},
                                Key=new_key
                            )
                            
                            # Delete old object
                            self.s3_client.delete_object(
                                Bucket=self.bucket_name,
                                Key=old_key
                            )
            else:
                # For single file, just copy and delete
                self.s3_client.copy_object(
                    Bucket=self.bucket_name,
                    CopySource={'Bucket': self.bucket_name, 'Key': old_path},
                    Key=new_path
                )
                
                self.s3_client.delete_object(
                    Bucket=self.bucket_name,
                    Key=old_path
                )
            
            return True, f"Successfully renamed to '{new_name}'"
            
        except Exception as e:
            logger.error(f"Rename error: {e}")
            return False, f"Rename failed: {str(e)}"
    
    def get_file_preview(self, file_path: str, max_size: int = 5 * 1024 * 1024) -> Dict:
        """Get preview of a file (text, image, csv, etc.)"""
        try:
            # Get file metadata
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            
            file_size = response['ContentLength']
            content_type = response.get('ContentType', '')
            
            # Check file size
            if file_size > max_size:
                return {
                    'type': 'error',
                    'message': f'File too large for preview (>{max_size/1024/1024:.1f}MB)'
                }
            
            # Get file content
            obj_response = self.s3_client.get_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            content = obj_response['Body'].read()
            
            # Determine file type and generate preview
            file_extension = file_path.split('.')[-1].lower()
            
            # Text files
            if file_extension in ['txt', 'log', 'md', 'py', 'js', 'html', 'css', 'json', 'xml', 'yaml', 'yml']:
                try:
                    text_content = content.decode('utf-8')
                    return {
                        'type': 'text',
                        'content': text_content[:10000],  # Limit to 10k chars
                        'truncated': len(text_content) > 10000
                    }
                except:
                    return {'type': 'error', 'message': 'Unable to decode text file'}
            
            # Images
            elif file_extension in ['jpg', 'jpeg', 'png', 'gif', 'bmp', 'svg']:
                if file_extension == 'svg':
                    return {
                        'type': 'svg',
                        'content': content.decode('utf-8')
                    }
                else:
                    try:
                        image = Image.open(BytesIO(content))
                        # Create thumbnail
                        image.thumbnail((800, 600), Image.Resampling.LANCZOS)
                        
                        # Convert to bytes
                        img_buffer = BytesIO()
                        image.save(img_buffer, format=image.format)
                        img_bytes = img_buffer.getvalue()
                        
                        return {
                            'type': 'image',
                            'content': img_bytes,
                            'format': image.format,
                            'size': image.size
                        }
                    except:
                        return {'type': 'error', 'message': 'Unable to process image'}
            
            # CSV files
            elif file_extension == 'csv':
                try:
                    df = pd.read_csv(BytesIO(content))
                    return {
                        'type': 'csv',
                        'content': df.head(100),  # First 100 rows
                        'shape': df.shape,
                        'columns': list(df.columns)
                    }
                except:
                    return {'type': 'error', 'message': 'Unable to parse CSV file'}
            
            # Excel files
            elif file_extension in ['xlsx', 'xls']:
                try:
                    df = pd.read_excel(BytesIO(content))
                    return {
                        'type': 'excel',
                        'content': df.head(100),
                        'shape': df.shape,
                        'columns': list(df.columns)
                    }
                except:
                    return {'type': 'error', 'message': 'Unable to parse Excel file'}
            
            # PDF preview not implemented (requires additional libraries)
            elif file_extension == 'pdf':
                return {
                    'type': 'pdf',
                    'message': 'PDF preview not available',
                    'size': file_size
                }
            
            # Unknown file type
            else:
                return {
                    'type': 'unknown',
                    'extension': file_extension,
                    'size': file_size
                }
                
        except Exception as e:
            logger.error(f"Preview error: {e}")
            return {'type': 'error', 'message': f'Preview failed: {str(e)}'}
    
    def generate_presigned_url(self, file_path: str, expiration: int = 3600) -> Optional[str]:
        """Generate a presigned URL for temporary file access"""
        try:
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': file_path
                },
                ExpiresIn=expiration
            )
            return url
        except Exception as e:
            logger.error(f"Presigned URL error: {e}")
            return None
    
    def get_folder_size(self, folder_path: str) -> Dict:
        """Calculate total size of a folder"""
        try:
            total_size = 0
            file_count = 0
            
            # Ensure folder path ends with /
            if not folder_path.endswith('/'):
                folder_path += '/'
            
            # List all objects in folder
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=folder_path
            )
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        total_size += obj['Size']
                        file_count += 1
            
            return {
                'total_size': total_size,
                'file_count': file_count,
                'formatted_size': self._format_size(total_size)
            }
            
        except Exception as e:
            logger.error(f"Folder size error: {e}")
            return {
                'total_size': 0,
                'file_count': 0,
                'formatted_size': '0 B',
                'error': str(e)
            }
    
    def search_files(self, search_term: str, path: str = "") -> List[Dict]:
        """Search for files and folders by name"""
        try:
            results = []
            
            # Format search path
            if path:
                search_prefix = self._format_path(path)
            else:
                search_prefix = self.app_prefix + "/"
            
            # List all objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(
                Bucket=self.bucket_name,
                Prefix=search_prefix
            )
            
            search_lower = search_term.lower()
            
            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        file_path = obj['Key']
                        file_name = file_path.split('/')[-1]
                        
                        # Check if filename matches search term
                        if search_lower in file_name.lower():
                            results.append({
                                'name': file_name,
                                'path': file_path,
                                'type': 'folder' if file_path.endswith('/') else 'file',
                                'size': self._format_size(obj['Size']),
                                'modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
                            })
            
            return results
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return []
    
    def get_file_metadata(self, file_path: str) -> Dict:
        """Get detailed metadata for a file"""
        try:
            response = self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=file_path
            )
            
            metadata = {
                'size': response['ContentLength'],
                'formatted_size': self._format_size(response['ContentLength']),
                'content_type': response.get('ContentType', 'Unknown'),
                'last_modified': response['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                'etag': response.get('ETag', '').strip('"'),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'metadata': response.get('Metadata', {})
            }
            
            # Add mime type info
            mime_type, _ = mimetypes.guess_type(file_path)
            if mime_type:
                metadata['mime_type'] = mime_type
            
            return metadata
            
        except Exception as e:
            logger.error(f"Metadata error: {e}")
            return {'error': str(e)}
    
    def _format_path(self, path: str) -> str:
        """Format path to ensure it starts with app_prefix"""
        if not path:
            return self.app_prefix + "/"
        
        path = path.strip("/")
        
        if not path.startswith(self.app_prefix):
            path = f"{self.app_prefix}/{path}"
        
        if not path.endswith("/"):
            path += "/"
        
        return path
    
    def _format_size(self, size_bytes: int) -> str:
        """Format file size in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"