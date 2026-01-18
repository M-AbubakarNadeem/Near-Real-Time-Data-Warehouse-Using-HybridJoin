import threading
import time
import queue
from datetime import datetime
import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys


class HybridJoinBatch:
    def __init__(self, db_config, batch_size=1000):
        """
        Initialize HYBRIDJOIN with batch processing
        
        Args:
            db_config: Database connection configuration
            batch_size: Number of records to insert at once
        """
        self.db_config = db_config
        self.batch_size = batch_size
        
        # Stream buffer
        self.stream_buffer = queue.Queue(maxsize=10000)
        
        # Batch buffer for database inserts
        self.insert_batch = []
        
        # Statistics
        self.total_stream_tuples = 0
        self.total_joined_tuples = 0
        self.total_loaded_tuples = 0
        self.total_skipped = 0
        
        # Control flags
        self.stream_complete = False
        self.running = True
        self.join_ready = threading.Event()
        
        # Master data
        self.customer_master = {}
        self.product_master = {}
        self.valid_dates = set()
        
        # Database connection
        self.db_conn = None
        
        print("HYBRIDJOIN initialized (BATCH INSERTION)")
        print(f"  Batch Size: {batch_size:,} records per insert")
    
    def connect_to_database(self):
        """Establish database connection"""
        try:
            self.db_conn = mysql.connector.connect(**self.db_config)
            if self.db_conn.is_connected():
                self.db_conn.autocommit = False
                print("Connected to MySQL database")
                return True
        except Error as e:
            print(f"Database connection error: {e}")
            return False
    
    def truncate_fact_table(self):
        """Truncate fact_sales table before loading"""
        print("\n" + "=" * 70)
        print("CHECKING EXISTING DATA")
        print("=" * 70)
        
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM fact_sales")
            existing_count = cursor.fetchone()[0]
            
            if existing_count > 0:
                print(f"WARNING: fact_sales table contains {existing_count:,} existing records")
                print("   These records will be DELETED before loading new data.")
                
                confirm = input("\nDo you want to proceed with TRUNCATE? (yes/no): ").strip().lower()
                
                if confirm != 'yes':
                    print("Operation cancelled by user.")
                    cursor.close()
                    return False
                
                print("\nTruncating fact_sales table...")
                cursor.execute("TRUNCATE TABLE fact_sales")
                self.db_conn.commit()
                print("fact_sales table cleared successfully!")
            else:
                print("fact_sales table is empty. Ready for loading.")
            
            cursor.close()
            return True
            
        except Exception as e:
            print(f"Error checking/truncating fact_sales: {e}")
            return False
    
    def load_master_data(self):
        """Load customer and product master data"""
        print("\nLoading master data into memory...")
        
        # Load customers
        cursor = self.db_conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM dim_customer")
        for row in cursor.fetchall():
            self.customer_master[row['customer_id']] = row
        print(f"Loaded {len(self.customer_master):,} customers")
        
        # Load valid dates from dim_date
        cursor.execute("SELECT DISTINCT date_id FROM dim_date")
        self.valid_dates = set(row['date_id'] for row in cursor.fetchall())
        print(f"Loaded {len(self.valid_dates):,} valid dates")
        
        cursor.close()
        
        # Load products from CSV
        try:
            products_df = pd.read_csv('product_master_data.csv')
            if products_df.columns[0].startswith('Unnamed'):
                products_df = products_df.drop(products_df.columns[0], axis=1)
            
            for _, row in products_df.iterrows():
                product_id = str(row['Product_ID']).strip()
                self.product_master[product_id] = {
                    'product_id': product_id,
                    'product_category': str(row['Product_Category']).strip(),
                    'price': float(row['price$']) if pd.notna(row['price$']) else 0.0,
                    'store_id': str(row['storeID']).strip(),
                    'supplier_id': str(row['supplierID']).strip()
                }
            print(f"Loaded {len(self.product_master):,} products with store/supplier mapping")
        except Exception as e:
            print(f"Error loading product data: {e}")
            return False
        
        return True
    
    def stream_reader_thread(self, csv_file, chunk_size=1000):
        """Read transactions from CSV"""
        self.join_ready.wait()
        
        print(f"[Stream Reader] Starting to read from {csv_file}...")
        
        try:
            chunk_iterator = pd.read_csv(csv_file, chunksize=chunk_size)
            
            for chunk_num, chunk in enumerate(chunk_iterator):
                if not self.running:
                    break
                
                if chunk.columns[0].startswith('Unnamed'):
                    chunk = chunk.drop(chunk.columns[0], axis=1)
                
                for _, row in chunk.iterrows():
                    if not self.running:
                        break
                    
                    transaction = {
                        'order_id': str(row['orderID']),
                        'customer_id': str(row['Customer_ID']),
                        'product_id': str(row['Product_ID']).strip(),
                        'quantity': int(row['quantity']),
                        'date': str(row['date'])
                    }
                    
                    self.stream_buffer.put(transaction)
                    self.total_stream_tuples += 1
                
                if (chunk_num + 1) % 50 == 0:
                    print(f"[Stream Reader] Read {self.total_stream_tuples:,} transactions...")
            
            print(f"[Stream Reader] Completed! Total: {self.total_stream_tuples:,}")
            
        except Exception as e:
            print(f"[Stream Reader] Error: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.stream_complete = True
    
    def flush_batch(self):
        """Insert current batch into database"""
        if not self.insert_batch:
            return 0
        
        try:
            cursor = self.db_conn.cursor()
            
            insert_query = """
                INSERT INTO fact_sales 
                (order_id, customer_id, product_id, date_id, store_id, supplier_id,
                 quantity, unit_price, total_amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            
            cursor.executemany(insert_query, self.insert_batch)
            self.db_conn.commit()
            
            count = len(self.insert_batch)
            self.total_loaded_tuples += count
            self.insert_batch = []
            
            cursor.close()
            return count
            
        except Exception as e:
            print(f"[Batch Insert Error] {e}")
            try:
                self.db_conn.rollback()
            except:
                pass
            self.insert_batch = []
            return 0
    
    def hybridjoin_thread(self):
        """Process stream and perform joins"""
        print("\n[HYBRIDJOIN] Starting join algorithm...")
        self.join_ready.set()
        
        processed = 0
        last_report = 0
        
        while self.running:
            try:
                stream_tuple = self.stream_buffer.get(timeout=0.5)
                processed += 1
                
                # Join with customer
                customer_id = stream_tuple['customer_id']
                if customer_id not in self.customer_master:
                    self.total_skipped += 1
                    continue
                
                # Join with product
                product_id = stream_tuple['product_id']
                if product_id not in self.product_master:
                    self.total_skipped += 1
                    continue
                
                product_data = self.product_master[product_id]
                
                # Convert date to date_id
                try:
                    date_obj = datetime.strptime(stream_tuple['date'], '%Y-%m-%d')
                    date_id = int(date_obj.strftime('%Y%m%d'))
                    
                    if date_id not in self.valid_dates:
                        self.total_skipped += 1
                        continue
                        
                except:
                    self.total_skipped += 1
                    continue
                
                # Calculate total amount
                unit_price = product_data['price']
                quantity = stream_tuple['quantity']
                total_amount = quantity * unit_price
                
                # Add to batch
                values = (
                    stream_tuple['order_id'],
                    stream_tuple['customer_id'],
                    stream_tuple['product_id'],
                    date_id,
                    product_data['store_id'],
                    product_data['supplier_id'],
                    quantity,
                    unit_price,
                    total_amount
                )
                
                self.insert_batch.append(values)
                self.total_joined_tuples += 1
                
                # Flush batch when it reaches batch_size
                if len(self.insert_batch) >= self.batch_size:
                    self.flush_batch()
                
                # Progress report every 10,000 records
                if processed - last_report >= 10000:
                    print(f"[HYBRIDJOIN] Processed: {processed:,} | "
                          f"Joined: {self.total_joined_tuples:,} | "
                          f"Loaded: {self.total_loaded_tuples:,} | "
                          f"Skipped: {self.total_skipped:,}")
                    last_report = processed
                    
            except queue.Empty:
                if self.stream_complete:
                    if self.insert_batch:
                        self.flush_batch()
                    
                    if self.stream_buffer.empty():
                        break
                continue
        
        # Final flush
        if self.insert_batch:
            print(f"[HYBRIDJOIN] Flushing final batch of {len(self.insert_batch)} records...")
            self.flush_batch()
        
        print(f"\n[HYBRIDJOIN] Completed!")
        print(f"  Processed: {processed:,}")
        print(f"  Joined: {self.total_joined_tuples:,}")
        print(f"  Loaded to DW: {self.total_loaded_tuples:,}")
        print(f"  Skipped: {self.total_skipped:,}")
    
    def run(self, csv_file):
        """Main execution"""
        print("\n" + "="*70)
        print("HYBRIDJOIN ETL PROCESS STARTING")
        print("="*70)
        
        if not self.connect_to_database():
            return False
        
        # TRUNCATE fact_sales table (NEW FEATURE)
        if not self.truncate_fact_table():
            return False
        
        if not self.load_master_data():
            return False
        
        print("\nStarting threads...")
        print("="*70)
        
        # Create threads
        thread1 = threading.Thread(
            target=self.stream_reader_thread,
            args=(csv_file,),
            name="StreamReader"
        )
        
        thread2 = threading.Thread(
            target=self.hybridjoin_thread,
            name="HybridJoin"
        )
        
        # Start threads
        thread2.start()
        time.sleep(0.1)
        thread1.start()
        
        # Wait for completion
        try:
            thread1.join()
            thread2.join()
        except KeyboardInterrupt:
            print("\n\nInterrupted by user. Shutting down...")
            self.running = False
            thread1.join()
            thread2.join()
        
        # Close connection
        if self.db_conn and self.db_conn.is_connected():
            self.db_conn.close()
            print("\nDatabase connection closed")
        
        print("\n" + "="*70)
        print("ETL PROCESS COMPLETED")
        print("="*70)
        print(f"Stream tuples read: {self.total_stream_tuples:,}")
        print(f"Tuples joined: {self.total_joined_tuples:,}")
        print(f"Tuples loaded to DW: {self.total_loaded_tuples:,}")
        print(f"Tuples skipped: {self.total_skipped:,}")
        
        if self.total_stream_tuples > 0:
            success_rate = (self.total_joined_tuples / self.total_stream_tuples) * 100
            print(f"Join success rate: {success_rate:.2f}%")
        
        return True


def main():
    """Main execution function"""
    print("\n")
    print("╔" + "="*68 + "╗")
    print("║" + " "*10 + "WALMART DATA WAREHOUSE - HYBRIDJOIN              " + " "*9 + "║")
    print("║" + " "*20 + "ETL Processing System" + " "*27 + "║")
    """print("║" + " "*18 + "(SAFE VERSION - WITH TRUNCATE)" + " "*20 + "║")"""
    print("╚" + "="*68 + "╝")
    print()
    
    print("Database Configuration:")
    print("-" * 70)
    host = input("Enter host: ").strip() or "localhost"
    database = input("Enter database name (default: walmart_dw): ").strip() or "walmart_dw"
    user = input("Enter username: ").strip() or "root"
    password = input("Enter password: ").strip()
    
    db_config = {
        'host': host,
        'database': database,
        'user': user,
        'password': password
    }
    
    csv_file = input("\nEnter transactional data CSV file (default: transactional_data.csv): ").strip()
    csv_file = csv_file or "transactional_data.csv"
    
    # Create and run
    hybrid_join = HybridJoinBatch(db_config, batch_size=1000)
    
    start_time = time.time()
    success = hybrid_join.run(csv_file)
    end_time = time.time()
    
    if success:
        elapsed = end_time - start_time
        print(f"\nTotal execution time: {elapsed:.2f} seconds ({elapsed/60:.2f} minutes)")
        if hybrid_join.total_loaded_tuples > 0:
            rate = hybrid_join.total_loaded_tuples / elapsed
            print(f"Processing rate: {rate:.2f} tuples/second")
    else:
        print("\nETL process failed")
        sys.exit(1)


if __name__ == "__main__":
    main()