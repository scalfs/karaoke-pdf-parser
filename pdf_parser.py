import pandas as pd
import tabula
import sqlite3
from pathlib import Path
import logging
import sys
import os

# Set up logging
os.makedirs('/app/logs', exist_ok=True)
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/app/logs/parser_debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def extract_karaoke_data(pdf_path, start_page=1, end_page=None):
    """
    Extract karaoke song data from PDF with detailed debugging
    """
    logging.info(f"Starting extraction from {pdf_path}")
    
    all_dataframes = []
    failed_pages = []
    
    # If end_page is not specified, try to process all pages
    if end_page is None:
        try:
            # Get total number of pages
            tables = tabula.read_pdf(
                pdf_path,
                pages='all',
                multiple_tables=False,  # Changed to False
                lattice=True
            )
            end_page = 316  # Hardcoded for now
            logging.info(f"Processing {end_page} pages in PDF")
        except Exception as e:
            logging.error(f"Error detecting total pages: {str(e)}")
            end_page = 316

    # Process pages one at a time to better handle issues
    for page_num in range(start_page, end_page + 1):
        logging.info(f"Processing page {page_num}")
        
        try:
            # Modified parameters for better table detection
            tables = tabula.read_pdf(
                pdf_path,
                pages=str(page_num),
                multiple_tables=False,  # Changed to False
                lattice=True,
                guess=True,  # Enable layout guessing
                pandas_options={
                    'header': None,
                    'error_bad_lines': False,
                    'warn_bad_lines': True
                }
            )
            
            if not isinstance(tables, list):
                tables = [tables]
            
            for table in tables:
                try:
                    if table.empty:
                        logging.warning(f"Empty table on page {page_num}")
                        failed_pages.append(page_num)
                        continue
                    
                    # Ensure correct number of columns
                    if len(table.columns) != 5:
                        # Try to fix column issues
                        if len(table.columns) > 5:
                            table = table.iloc[:, :5]  # Take first 5 columns
                        else:
                            logging.warning(f"Wrong number of columns on page {page_num}: {len(table.columns)}")
                            failed_pages.append(page_num)
                            continue
                    
                    # Drop header row if it exists
                    if 'Interprete' in str(table.iloc[0]) or 'Cod' in str(table.iloc[0]):
                        table = table.iloc[1:]
                    
                    # Assign column names
                    table.columns = ['Interprete', 'Cod', 'Titulo', 'Inicio da letra', 'Idioma']
                    
                    # Clean up data
                    table = table.fillna('')
                    for col in table.columns:
                        table[col] = table[col].astype(str).str.strip()
                    
                    # Add page number
                    table['Page'] = page_num
                    
                    # Basic validation
                    if len(table) > 0:
                        all_dataframes.append(table)
                        logging.info(f"Successfully processed page {page_num} with {len(table)} rows")
                    else:
                        logging.warning(f"No valid data on page {page_num}")
                        failed_pages.append(page_num)
                    
                except Exception as e:
                    logging.error(f"Error processing table on page {page_num}: {str(e)}")
                    failed_pages.append(page_num)
                    
        except Exception as e:
            logging.error(f"Error processing page {page_num}: {str(e)}")
            failed_pages.append(page_num)
    
    if not all_dataframes:
        raise Exception("No data was successfully extracted")
    
    # Combine all successful extractions
    df = pd.concat(all_dataframes, ignore_index=True)
    
    # Save debug CSV to data directory
    os.makedirs('/app/data', exist_ok=True)
    df.to_csv('/app/data/extracted_data_debug.csv', index=False)
    
    # Log summary
    logging.info(f"Extraction complete:")
    logging.info(f"Total rows extracted: {len(df)}")
    logging.info(f"Failed pages: {failed_pages}")
    
    return df, failed_pages

def save_to_sqlite(df, db_path):
    """
    Save extracted data to SQLite database with validation
    """
    logging.info(f"Saving data to {db_path}")
    
    # Ensure data directory exists
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Create table
        conn.execute('''
        CREATE TABLE IF NOT EXISTS karaoke_songs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            artist TEXT NOT NULL,
            code TEXT NOT NULL,
            title TEXT NOT NULL,
            lyrics_start TEXT,
            language TEXT,
            page_number INTEGER,
            UNIQUE(code)
        )
        ''')
        
        # Insert data
        df.to_sql(
            'karaoke_songs', 
            conn, 
            if_exists='replace',
            index=False,
            dtype={
                'Interprete': 'TEXT',
                'Cod': 'TEXT',
                'Titulo': 'TEXT',
                'Inicio da letra': 'TEXT',
                'Idioma': 'TEXT',
                'Page': 'INTEGER'
            }
        )
        
        # Verify insertion
        count = conn.execute("SELECT COUNT(*) FROM karaoke_songs").fetchone()[0]
        logging.info(f"Successfully saved {count} rows to database")
        
    except Exception as e:
        logging.error(f"Database error: {str(e)}")
        raise
    finally:
        conn.close()

def main():
    pdf_path = 'karaoke_list.pdf'
    db_path = '/app/data/karaoke.db'
    
    try:
        df, failed_pages = extract_karaoke_data(pdf_path)
        save_to_sqlite(df, db_path)
        
        # Generate summary file
        with open('/app/data/extraction_summary.txt', 'w') as f:
            f.write(f"Total songs extracted: {len(df)}\n")
            f.write(f"Failed pages: {failed_pages}\n")
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()