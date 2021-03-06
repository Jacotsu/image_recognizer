init_pragmas = '''
PRAGMA jorunal_mode = WAL;
PRAGMA threads = 8;
'''

create_metadata_table = '''
CREATE TABLE IF NOT EXISTS files_metadata (
    file_hash BINARY(16) NOT NULL,
    image_signature BLOB NOT NULL,
    PRIMARY KEY (file_hash)
);
'''

create_paths_table = '''
CREATE TABLE IF NOT EXISTS files_paths (
    id INT PRIMARY KEY,
    file_hash BINARY(16) NOT NULL,
    file_path TEXT NOT NULL,
    UNIQUE(file_path),
    FOREIGN KEY (file_hash) REFERENCES
    files_metadata(file_hash)
);
'''

insert_image_metadata = '''
INSERT INTO files_metadata(
    file_hash,
    image_signature)
VALUES (?, ?);
'''

get_image_metadata = '''
SELECT file_hash, image_signature
FROM file_metadata
WHERE file_hash=?;
'''

get_all_images = '''
SELECT file_hash, image_signature
FROM files_metadata;
'''

check_hash_existence = '''
SELECT 1
FROM files_metadata
WHERE file_hash = ?;
'''

insert_path = '''
INSERT INTO files_paths(file_hash, file_path)
VALUES (?1, ?2);
'''

delete_path = '''
DELETE FROM files_paths
WHERE id=?;
'''

get_paths = '''
SELECT file_path
FROM files_paths
WHERE file_hash=?;
'''

get_all_paths = '''
SELECT id, file_path
FROM files_paths;
'''
