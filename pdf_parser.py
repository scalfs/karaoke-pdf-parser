import pandas as pd
import tabula
import sqlite3
from pathlib import Path
import logging
import sys

# Set up logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('parser_debug.log'),
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
                multiple_tables=True,
                lattice=True
            )
            end_page = len(tables)
            logging.info(f"Detected {end_page} pages in PDF")
        except Exception as e:
            logging.error(f"Error detecting total pages: {str(e)}")
            end_page = 316  # Fallback to known number of pages

    # Process pages in smaller chunks to avoid memory issues
    chunk_size = 10
    for chunk_start in range(start_page, end_page + 1, chunk_size):
        chunk_end = min(chunk_start + chunk_size - 1, end_page)
        logging.info(f"Processing pages {chunk_start} to {chunk_end}")
        
        try:
            # Read chunk of pages
            tables = tabula.read_pdf(
                pdf_path,
                pages=f"{chunk_start}-{chunk_end}",
                multiple_tables=True,
                lattice=True,
                guess=False,
                pandas_options={
                    'header': None  # We'll handle headers manually
                }
            )
            
            logging.info(f"Found {len(tables)} tables in pages {chunk_start}-{chunk_end}")
            
            # Process each table in the chunk
            for page_idx, table in enumerate(tables, start=chunk_start):
                try:
                    # Check if table is empty
                    if table.empty:
                        logging.warning(f"Empty table on page {page_idx}")
                        failed_pages.append(page_idx)
                        continue
                    
                    # Drop the header row if it exists
                    if 'Interprete' in str(table.iloc[0]) or 'Cod' in str(table.iloc[0]):
                        table = table.iloc[1:]
                    
                    # Assign correct column names
                    table.columns = ['Interprete', 'Cod', 'Titulo', 'Inicio da letra', 'Idioma']
                    
                    # Basic validation
                    if len(table.columns) != 5:
                        logging.warning(f"Wrong number of columns on page {page_idx}: {len(table.columns)}")
                        failed_pages.append(page_idx)
                        continue
                    
                    # Clean up data
                    table = table.fillna('')
                    table['Cod'] = table['Cod'].astype(str).str.strip()
                    table['Interprete'] = table['Interprete'].str.strip()
                    table['Titulo'] = table['Titulo'].str.strip()
                    table['Inicio da letra'] = table['Inicio da letra'].str.strip()
                    table['Idioma'] = table['Idioma'].str.strip()
                    
                    # Add page number for debugging
                    table['Page'] = page_idx
                    
                    all_dataframes.append(table)
                    logging.info(f"Successfully processed page {page_idx} with {len(table)} rows")
                    
                except Exception as e:
                    logging.error(f"Error processing table on page {page_idx}: {str(e)}")
                    failed_pages.append(page_idx)
                    
        except Exception as e:
            logging.error(f"Error processing chunk {chunk_start}-{chunk_end}: {str(e)}")
            failed_pages.extend(range(chunk_start, chunk_end + 1))
    
    if not all_dataframes:
        raise Exception("No data was successfully extracted")
    
    # Combine all successful extractions
    df = pd.concat(all_dataframes, ignore_index=True)
    
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
    pdf_path = 'karaoke_list.pdf'  # Update with your PDF path
    db_path = 'karaoke.db'
    
    # Optional: Process specific page range
    start_page = 1
    end_page = 316  # or None to auto-detect
    
    try:
        df, failed_pages = extract_karaoke_data(pdf_path, start_page, end_page)
        
        # Save extracted data
        save_to_sqlite(df, db_path)
        
        # Generate debug summary
        print("\nExtraction Summary:")
        print(f"Total songs extracted: {len(df)}")
        print(f"Failed pages: {failed_pages}")
        print(f"Sample of extracted data:")
        print(df.head())
        
        # Save debug info to file
        df.to_csv('extracted_data_debug.csv', index=False)
        
    except Exception as e:
        logging.error(f"Fatal error: {str(e)}")
        raise

if __name__ == "__main__":
    main()