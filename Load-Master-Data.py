import pandas as pd
import mysql.connector
from mysql.connector import Error
import sys


def get_db_connection():
    """Get database connection with user input"""
    print("=" * 60)
    print("MySQL Database Connection")
    print("=" * 60)

    host = input("Enter host: ").strip() or "localhost"
    database = (
        input("Enter database name (default: walmart_dw): ").strip() or "walmart_dw"
    )
    user = input("Enter username: ").strip() or "root"
    password = input("Enter password: ").strip()

    try:
        conn = mysql.connector.connect(
            host=host, database=database, user=user, password=password
        )

        if conn.is_connected():
            print("\n✓ Successfully connected to MySQL database")
            db_info = conn.get_server_info()
            print(f"  MySQL Server version: {db_info}")
            return conn
    except Error as e:
        print(f"\n✗ Error connecting to MySQL: {e}")
        return None


def truncate_tables(cursor):
    """Truncate dimension tables before loading"""
    print("\n" + "=" * 60)
    print("CLEARING EXISTING DATA")
    print("=" * 60)
    print("WARNING: This will delete ALL existing data in dimension tables!")

    confirm = input("\nDo you want to proceed? (yes/no): ").strip().lower()

    if confirm != "yes":
        print("Operation cancelled by user.")
        return False

    tables_to_truncate = [
        "fact_sales",  # Must truncate fact table first (foreign keys)
        "dim_customer",
        "dim_product",
        "dim_store",
        "dim_supplier",
    ]

    try:
        # Disable foreign key checks temporarily
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

        for table in tables_to_truncate:
            try:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"Truncated {table}")
            except Exception as e:
                print(f"Could not truncate {table}: {e}")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

        print("\nAll tables cleared successfully!")
        return True

    except Exception as e:
        print(f"\nError during truncate: {e}")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1")  # Re-enable even on error
        return False


def load_customer_data(cursor, csv_file):
    """Load customer master data"""
    print("\n" + "=" * 60)
    print("Loading Customer Master Data...")
    print("=" * 60)

    try:
        # Read CSV file
        customers = pd.read_csv(csv_file)
        print(f"Found {len(customers)} customer records in CSV")

        # Remove the unnamed index column if exists
        if customers.columns[0].startswith("Unnamed"):
            customers = customers.drop(customers.columns[0], axis=1)

        inserted = 0
        errors = 0

        for idx, row in customers.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO dim_customer 
                    (customer_id, gender, age, occupation, city_category, 
                     stay_in_current_city_years, marital_status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        str(row["Customer_ID"]).strip(),
                        str(row["Gender"]).strip(),
                        str(row["Age"]).strip(),
                        str(row["Occupation"]).strip(),
                        str(row["City_Category"]).strip(),
                        str(row["Stay_In_Current_City_Years"]).strip(),
                        str(row["Marital_Status"]).strip(),
                    ),
                )
                inserted += 1

            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  Error on row {idx}: {e}")

            # Progress indicator
            if (idx + 1) % 1000 == 0:
                print(f"  Processed {idx + 1}/{len(customers)} records...")

        print(f"\nCustomer Data Summary:")
        print(f"  Inserted: {inserted}")
        print(f"  Errors: {errors}")
        return True

    except FileNotFoundError:
        print(f"File not found: {csv_file}")
        return False
    except Exception as e:
        print(f"Error loading customer data: {e}")
        import traceback

        traceback.print_exc()
        return False


def load_product_data(cursor, csv_file):
    """Load product master data including stores and suppliers"""
    print("\n" + "=" * 60)
    print("Loading Product Master Data...")
    print("=" * 60)

    try:
        # Read CSV file
        products = pd.read_csv(csv_file)
        print(f"Found {len(products)} product records in CSV")

        # Remove the unnamed index column if exists
        if products.columns[0].startswith("Unnamed"):
            products = products.drop(products.columns[0], axis=1)

        # Load Products
        print("\n[1/3] Loading products...")
        product_inserted = 0
        product_errors = 0

        for idx, row in products.iterrows():
            try:
                price_value = float(row["price$"]) if pd.notna(row["price$"]) else 0.0

                cursor.execute(
                    """
                    INSERT INTO dim_product (product_id, product_category, price)
                    VALUES (%s, %s, %s)
                """,
                    (
                        str(row["Product_ID"]).strip(),
                        str(row["Product_Category"]).strip(),
                        price_value,
                    ),
                )
                product_inserted += 1

            except Exception as e:
                product_errors += 1
                if product_errors <= 5:
                    print(f"  Error on row {idx}: {e}")

            if (idx + 1) % 500 == 0:
                print(f"  Processed {idx + 1}/{len(products)} products...")

        print(f"\nProducts - Inserted: {product_inserted}, Errors: {product_errors}")

        # Load Stores
        print("\n[2/3] Loading stores...")
        stores = products[["storeID", "storeName"]].drop_duplicates("storeID")
        store_inserted = 0
        store_errors = 0

        for idx, row in stores.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO dim_store (store_id, store_name)
                    VALUES (%s, %s)
                """,
                    (str(row["storeID"]).strip(), str(row["storeName"]).strip()),
                )
                store_inserted += 1

            except Exception as e:
                store_errors += 1
                if store_errors <= 5:
                    print(f"  Error: {e}")

        print(f"Stores - Inserted: {store_inserted}, Errors: {store_errors}")

        # Load Suppliers
        print("\n[3/3] Loading suppliers...")
        suppliers = products[["supplierID", "supplierName"]].drop_duplicates(
            "supplierID"
        )
        supplier_inserted = 0
        supplier_errors = 0

        for idx, row in suppliers.iterrows():
            try:
                cursor.execute(
                    """
                    INSERT INTO dim_supplier (supplier_id, supplier_name)
                    VALUES (%s, %s)
                """,
                    (str(row["supplierID"]).strip(), str(row["supplierName"]).strip()),
                )
                supplier_inserted += 1

            except Exception as e:
                supplier_errors += 1
                if supplier_errors <= 5:
                    print(f"  Error: {e}")

        print(f"Suppliers - Inserted: {supplier_inserted}, Errors: {supplier_errors}")

        return True

    except FileNotFoundError:
        print(f"File not found: {csv_file}")
        return False
    except Exception as e:
        print(f"Error loading product data: {e}")
        import traceback

        traceback.print_exc()
        return False


def verify_data(cursor):
    """Verify loaded data"""
    print("\n" + "=" * 60)
    print("Data Verification Summary")
    print("=" * 60)

    tables = [
        ("dim_customer", "Customers"),
        ("dim_product", "Products"),
        ("dim_store", "Stores"),
        ("dim_supplier", "Suppliers"),
        ("dim_date", "Date Records"),
    ]

    for table, description in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"{description:20s}: {count:,} records")
        except Exception as e:
            print(f"{description:20s}: Error - {e}")


def main():
    """Main function"""
    print("\n")
    print("╔" + "=" * 58 + "╗")
    print("║" + " " * 10 + "WALMART DATA WAREHOUSE" + " " * 26 + "║")
    print("║" + " " * 15 + "Master Data Loader" + " " * 25 + "║")
    print("║" + " " * 10 + "(SAFE VERSION - WITH TRUNCATE)" + " " * 18 + "║")
    print("╚" + "=" * 58 + "╝")
    print()

    # Get database connection
    conn = get_db_connection()
    if not conn:
        print("\n✗ Failed to connect to database. Exiting...")
        sys.exit(1)

    cursor = conn.cursor()

    try:
        # TRUNCATE existing data
        if not truncate_tables(cursor):
            print("\n✗ Failed to truncate tables. Exiting...")
            sys.exit(1)

        conn.commit()

        # Load customer data
        customer_file = input(
            "\nEnter customer CSV file path (default: customer_master_data.csv): "
        ).strip()
        customer_file = customer_file or "customer_master_data.csv"

        if load_customer_data(cursor, customer_file):
            conn.commit()
            print("✓ Customer data committed to database")
        else:
            print("✗ Failed to load customer data")
            conn.rollback()
            sys.exit(1)

        # Load product data
        product_file = input(
            "\nEnter product CSV file path (default: product_master_data.csv): "
        ).strip()
        product_file = product_file or "product_master_data.csv"

        if load_product_data(cursor, product_file):
            conn.commit()
            print("✓ Product data committed to database")
        else:
            print("Failed to load product data")
            conn.rollback()
            sys.exit(1)

        # Verify data
        verify_data(cursor)

        print("\n" + "=" * 60)
        print("Master Data Loaded Successfully!")
        print("=" * 60)
        print("\nDatabase is now ready for HYBRIDJOIN ETL process.")
        print("   Run: python Hybrid-Join-BATCH.py")
        print()

    except Exception as e:
        print(f"\nUnexpected Error: {e}")
        import traceback

        traceback.print_exc()
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        print("\nDatabase connection closed.")


if __name__ == "__main__":
    main()
