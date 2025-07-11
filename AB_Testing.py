import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from pandasql import sqldf
import os

# Define the column names as constants for easy management
TBL_COLUMNS = [
    'ACID', '1099_Type', '1099_Amt', '1099_Source', 'Date_of_Transaction', 
    'Borrower_CIF', 'Cosigner_CIF'
]

TRAN_COLUMNS = [
    'ACID', 'Loan_Number', 'Borrower_CIF', 'Value_Date', 'UTC', 'Tran_Date', 
    'Tran_ID', 'Tran_Total', 'Tran_Prin', 'Tran_INT', 'Tran_Fee', 
    'Agent_ID_System_Processes_ID', 'Tran_Description', 'Tran_Remarks', 'Cosigner_CIF'
]

class DataProcessorApp:
    def __init__(self, root):
        """Initialize the application."""
        self.root = root
        self.root.title("1099 Data Processing Tool")
        self.root.geometry("800x600")

        # Style configuration
        self.style = ttk.Style(self.root)
        self.style.theme_use("clam") # You can try other themes like 'alt', 'default', 'classic'

        # File paths
        self.tbl_file_path = tk.StringVar()
        self.tran_file_path = tk.StringVar()

        # DataFrames
        self.df_1099MTbl = None
        self.df_1099MTran = None

        self.create_widgets()

    def log_message(self, message):
        """Adds a message to the status log."""
        self.status_log.config(state=tk.NORMAL)
        self.status_log.insert(tk.END, message + "\n")
        self.status_log.see(tk.END) # Scroll to the end
        self.status_log.config(state=tk.DISABLED)
        self.root.update_idletasks()

    def create_widgets(self):
        """Create and layout all the GUI widgets."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # --- File Selection Frame ---
        file_frame = ttk.LabelFrame(main_frame, text="Step 1: Upload Input Files", padding="10")
        file_frame.pack(fill=tk.X, padx=5, pady=5)
        file_frame.grid_columnconfigure(1, weight=1)

        # 1099MTbl file
        ttk.Label(file_frame, text="1099MTbl File (.dat):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tbl_file_path, state="readonly").grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tbl_file).grid(row=0, column=2, padx=5, pady=5)

        # 1099MTran file
        ttk.Label(file_frame, text="1099MTran File (.dat):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tran_file_path, state="readonly").grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tran_file).grid(row=1, column=2, padx=5, pady=5)

        # --- Processing Frame ---
        process_frame = ttk.LabelFrame(main_frame, text="Step 2: Process Data", padding="10")
        process_frame.pack(fill=tk.X, padx=5, pady=10)

        process_button = ttk.Button(process_frame, text="Process Files and Generate Report", command=self.process_data)
        process_button.pack(pady=10, padx=20, fill=tk.X)

        # --- Status Log Frame ---
        status_frame = ttk.LabelFrame(main_frame, text="Status Log", padding="10")
        status_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        self.status_log = tk.Text(status_frame, height=15, state=tk.DISABLED, wrap=tk.WORD, bg="#f0f0f0")
        scrollbar = ttk.Scrollbar(status_frame, command=self.status_log.yview)
        self.status_log.config(yscrollcommand=scrollbar.set)
        
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def browse_file(self, path_var, title):
        """Opens a file dialog to select a .dat file."""
        file_path = filedialog.askopenfilename(
            title=title,
            filetypes=(("Data Files", "*.dat"), ("All files", "*.*"))
        )
        if file_path:
            path_var.set(file_path)
            self.log_message(f"Selected {os.path.basename(file_path)} for {title}")

    def browse_tbl_file(self):
        self.browse_file(self.tbl_file_path, "1099MTbl")

    def browse_tran_file(self):
        self.browse_file(self.tran_file_path, "1099MTran")

    def load_and_transform_data(self):
        """Loads data from files and applies necessary transformations."""
        try:
            self.log_message("Loading 1099MTbl file...")
            self.df_1099MTbl = pd.read_csv(
                self.tbl_file_path.get(),
                sep='|',
                header=None,
                names=TBL_COLUMNS,
                dtype=str # Load all as string to preserve leading zeros
            )
            self.log_message(f"-> Success: Loaded {len(self.df_1099MTbl)} rows into 1099MTbl DataFrame.")

            self.log_message("Loading 1099MTran file...")
            self.df_1099MTran = pd.read_csv(
                self.tran_file_path.get(),
                sep='|',
                header=None,
                names=TRAN_COLUMNS,
                dtype=str # Load all as string
            )
            self.log_message(f"-> Success: Loaded {len(self.df_1099MTran)} rows into 1099MTran DataFrame.")
            
            # --- Data Transformation ---
            self.log_message("Applying data transformations...")

            # Format dates (mm/dd/yyyy)
            date_cols_tbl = ['Date_of_Transaction']
            for col in date_cols_tbl:
                self.df_1099MTbl[col] = pd.to_datetime(self.df_1099MTbl[col], errors='coerce').dt.strftime('%m/%d/%Y')
            
            date_cols_tran = ['Value_Date', 'Tran_Date']
            for col in date_cols_tran:
                self.df_1099MTran[col] = pd.to_datetime(self.df_1099MTran[col], errors='coerce').dt.strftime('%m/%d/%Y')
            
            self.log_message("-> Dates formatted to MM/DD/YYYY.")

            # Pad with leading zeros
            self.df_1099MTbl['Borrower_CIF'] = self.df_1099MTbl['Borrower_CIF'].str.zfill(10)
            self.df_1099MTbl['Cosigner_CIF'] = self.df_1099MTbl['Cosigner_CIF'].str.zfill(10)
            
            self.df_1099MTran['Borrower_CIF'] = self.df_1099MTran['Borrower_CIF'].str.zfill(10)
            self.df_1099MTran['Cosigner_CIF'] = self.df_1099MTran['Cosigner_CIF'].str.zfill(10)
            self.df_1099MTran['Loan_Number'] = self.df_1099MTran['Loan_Number'].str.zfill(15)
            
            self.log_message("-> Padded CIF and Loan_Number columns with leading zeros.")
            self.log_message("Transformation complete.")
            return True

        except Exception as e:
            messagebox.showerror("Loading Error", f"Failed to load or transform files.\n\nError: {e}")
            self.log_message(f"ERROR: {e}")
            return False

    def execute_sql_query(self):
        """Executes the SQL query on the loaded DataFrames."""
        self.log_message("Executing SQL query...")
        
        # Make DataFrames available to pandasql
        df_1099MTbl = self.df_1099MTbl
        df_1099MTran = self.df_1099MTran

        # ===================================================================
        # TODO: REPLACE THIS WITH YOUR ACTUAL PSQL QUERY
        #
        # This is a sample query that joins the two tables on ACID and CIF.
        # You will replace the content of this `query` variable with your own.
        # ===================================================================
        query = """
        SELECT
            t1.ACID,
            t1."1099_Type",
            t1."1099_Amt",
            t2.Loan_Number,
            t2.Tran_Date,
            t2.Tran_Description,
            t1.Borrower_CIF,
            t1.Cosigner_CIF
        FROM
            df_1099MTbl t1
        JOIN
            df_1099MTran t2 ON t1.ACID = t2.ACID AND t1.Borrower_CIF = t2.Borrower_CIF
        WHERE
            t1."1099_Type" = 'INT' -- Example filter
        ORDER BY
            t2.Tran_Date;
        """
        
        try:
            # The sqldf function requires a function wrapper to access local variables
            pysqldf = lambda q: sqldf(q, locals())
            result_df = pysqldf(query)
            
            self.log_message(f"-> Success: Query executed, {len(result_df)} rows in result set.")
            return result_df
            
        except Exception as e:
            messagebox.showerror("SQL Query Error", f"The SQL query failed to execute.\n\nError: {e}")
            self.log_message(f"ERROR executing SQL: {e}")
            return None

    def save_to_excel(self, df):
        """Saves the final DataFrame to an Excel file."""
        self.log_message("Saving results to Excel...")
        
        save_path = filedialog.asksaveasfilename(
            title="Save Report As",
            defaultextension=".xlsx",
            filetypes=(("Excel Files", "*.xlsx"), ("All files", "*.*"))
        )
        
        if not save_path:
            self.log_message("Save operation cancelled by user.")
            return

        try:
            df.to_excel(save_path, index=False)
            self.log_message(f"SUCCESS! Report saved to:\n{save_path}")
            messagebox.showinfo("Success", f"Report successfully generated and saved to\n{save_path}")
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save the Excel file.\n\nError: {e}")
            self.log_message(f"ERROR saving Excel file: {e}")

    def process_data(self):
        """Main function to orchestrate the entire data processing workflow."""
        # 1. Validate inputs
        if not self.tbl_file_path.get() or not self.tran_file_path.get():
            messagebox.showerror("Input Error", "Please select both 1099MTbl and 1099MTran files.")
            return

        # 2. Load and Transform Data
        if not self.load_and_transform_data():
            return # Stop if loading failed

        # 3. Execute SQL Query
        final_df = self.execute_sql_query()
        if final_df is None:
            return # Stop if query failed

        if final_df.empty:
            self.log_message("WARNING: The query produced no results. Nothing to save.")
            messagebox.showwarning("No Results", "The query executed successfully but produced no results.")
            return

        # 4. Save to Excel
        self.save_to_excel(final_df)


if __name__ == "__main__":
    root = tk.Tk()
    app = DataProcessorApp(root)
    root.mainloop()
