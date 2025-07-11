import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from pandasql import sqldf
import os

# --- Constants for column names for clarity and easy maintenance ---
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
        self.root.geometry("800x650")

        self.style = ttk.Style(self.root)
        self.style.theme_use("clam")

        self.tbl_file_path = tk.StringVar()
        self.tran_file_path = tk.StringVar()
        self.df_1099MTbl = None
        self.df_1099MTran = None

        self.create_widgets()

    def log_message(self, message):
        """Adds a message to the status log text widget."""
        self.status_log.config(state=tk.NORMAL)
        self.status_log.insert(tk.END, message + "\n")
        self.status_log.see(tk.END)
        self.status_log.config(state=tk.DISABLED)
        self.root.update_idletasks()

    def create_widgets(self):
        """Create and layout all the GUI widgets."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Frame for File Selection
        file_frame = ttk.LabelFrame(main_frame, text="Step 1: Upload Input Files", padding="10")
        file_frame.pack(fill=tk.X, padx=5, pady=5)
        file_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(file_frame, text="1099MTbl File (.dat):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tbl_file_path, state="readonly").grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tbl_file).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="1099MTran File (.dat):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tran_file_path, state="readonly").grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tran_file).grid(row=1, column=2, padx=5, pady=5)

        # Frame for the Main Action Button
        process_frame = ttk.LabelFrame(main_frame, text="Step 2: Process Data", padding="10")
        process_frame.pack(fill=tk.X, padx=5, pady=10)
        process_button = ttk.Button(process_frame, text="Process Files and Generate Final Report", command=self.process_data)
        process_button.pack(pady=10, padx=20, fill=tk.X)

        # Frame for the Status Log
        status_frame = ttk.LabelFrame(main_frame, text="Status Log", padding="10")
        status_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.status_log = tk.Text(status_frame, height=15, state=tk.DISABLED, wrap=tk.WORD, bg="#f0f0f0", relief=tk.SOLID, borderwidth=1)
        scrollbar = ttk.Scrollbar(status_frame, command=self.status_log.yview)
        self.status_log.config(yscrollcommand=scrollbar.set)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.status_log.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

    def browse_file(self, path_var, title):
        file_path = filedialog.askopenfilename(title=f"Select {title} File", filetypes=(("Data Files", "*.dat"), ("All files", "*.*")))
        if file_path:
            path_var.set(file_path)
            self.log_message(f"Selected {os.path.basename(file_path)} for {title}")

    def browse_tbl_file(self):
        self.browse_file(self.tbl_file_path, "1099MTbl")

    def browse_tran_file(self):
        self.browse_file(self.tran_file_path, "1099MTran")

    def load_and_transform_data(self):
        """Loads data from files and applies transformations."""
        try:
            self.log_message("--- Starting Data Loading and Transformation ---")
            self.df_1099MTbl = pd.read_csv(self.tbl_file_path.get(), sep='|', header=None, names=TBL_COLUMNS, dtype=str, skipinitialspace=True)
            self.log_message(f"-> Loaded {len(self.df_1099MTbl)} rows from 1099MTbl.")
            self.df_1099MTran = pd.read_csv(self.tran_file_path.get(), sep='|', header=None, names=TRAN_COLUMNS, dtype=str, skipinitialspace=True)
            self.log_message(f"-> Loaded {len(self.df_1099MTran)} rows from 1099MTran.")
            self.log_message("Applying data transformations...")
            for col in ['Date_of_Transaction']:
                self.df_1099MTbl[col] = pd.to_datetime(self.df_1099MTbl[col], errors='coerce').dt.strftime('%m/%d/%Y')
            for col in ['Value_Date', 'Tran_Date']:
                self.df_1099MTran[col] = pd.to_datetime(self.df_1099MTran[col], errors='coerce').dt.strftime('%m/%d/%Y')
            self.df_1099MTbl['Borrower_CIF'] = self.df_1099MTbl['Borrower_CIF'].str.strip().str.zfill(10)
            self.df_1099MTbl['Cosigner_CIF'] = self.df_1099MTbl['Cosigner_CIF'].str.strip().str.zfill(10)
            self.df_1099MTran['Borrower_CIF'] = self.df_1099MTran['Borrower_CIF'].str.strip().str.zfill(10)
            self.df_1099MTran['Cosigner_CIF'] = self.df_1099MTran['Cosigner_CIF'].str.strip().str.zfill(10)
            self.df_1099MTran['Loan_Number'] = self.df_1099MTran['Loan_Number'].str.strip().str.zfill(15)
            self.log_message("-> Transformations complete.")
            return True
        except Exception as e:
            messagebox.showerror("Loading Error", f"Failed to load or transform files.\n\nError: {e}")
            self.log_message(f"ERROR: {e}")
            return False

    def execute_sql(self, query, query_name, tables):
        """Executes a SQL query against a dictionary of DataFrames."""
        self.log_message(f"Executing {query_name}...")
        try:
            # The 'tables' dictionary provides the scope for pandasql
            pysqldf = lambda q: sqldf(q, tables)
            result_df = pysqldf(query)
            self.log_message(f"-> Success: {query_name} returned {len(result_df)} rows.")
            return result_df
        except Exception as e:
            messagebox.showerror("SQL Query Error", f"The query '{query_name}' failed.\n\nError: {e}")
            self.log_message(f"ERROR executing {query_name}: {e}")
            return None

    def save_final_report(self, df):
        """Saves a single DataFrame to an Excel file."""
        self.log_message("Saving final report to Excel...")
        save_path = filedialog.asksaveasfilename(
            title="Save Final Report As",
            defaultextension=".xlsx",
            filetypes=(("Excel Files", "*.xlsx"), ("All files", "*.*"))
        )
        if not save_path:
            self.log_message("Save operation cancelled by user.")
            return

        try:
            df.to_excel(save_path, index=False, sheet_name="Final_Report")
            self.log_message(f"--- SUCCESS! Report saved to: {save_path} ---")
            messagebox.showinfo("Success", f"Report successfully generated and saved to\n{save_path}")
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save the Excel file.\n\nError: {e}")
            self.log_message(f"ERROR saving Excel file: {e}")

    def process_data(self):
        """Orchestrates the two-step query process and saves the final result."""
        # 1. Validate inputs
        if not self.tbl_file_path.get() or not self.tran_file_path.get():
            messagebox.showerror("Input Error", "Please select both input files.")
            return

        # 2. Load and Transform Data
        if not self.load_and_transform_data():
            return

        # =================================================================================
        # ===> STEP 1: DEFINE YOUR INTERMEDIATE QUERY (QUERY 1) <===
        # This query runs on the initial DataFrames: 'df_1099MTbl' and 'df_1099MTran'
        # =================================================================================
        QUERY_1_SQL = """
            -- Example: Join the two tables
            SELECT
                t1.*, -- Select all columns from the transaction table
                t2."1099_Type",
                t2."1099_Amt"
            FROM
                df_1099MTran t1
            LEFT JOIN
                df_1099MTbl t2 ON t1.ACID = t2.ACID;
        """

        # Execute Query 1
        tables_for_query1 = {
            'df_1099MTbl': self.df_1099MTbl,
            'df_1099MTran': self.df_1099MTran
        }
        intermediate_df = self.execute_sql(QUERY_1_SQL, "Intermediate Query (Query 1)", tables_for_query1)

        # Stop if the first query failed or returned no results
        if intermediate_df is None or intermediate_df.empty:
            self.log_message("WARNING: Intermediate query (Query 1) produced no results. Processing stopped.")
            messagebox.showwarning("No Results", "The first query produced no data, so the final report cannot be generated.")
            return

        # =================================================================================
        # ===> STEP 2: DEFINE YOUR FINAL REPORT QUERY (QUERY 2) <===
        # This query runs on the result of Query 1. The table name MUST be 'intermediate_df'.
        # =================================================================================
        QUERY_2_SQL = """
            -- Example: Filter the intermediate result for specific criteria
            SELECT
                Loan_Number,
                Borrower_CIF,
                Tran_Date,
                Tran_Description,
                "1099_Type",
                "1099_Amt"
            FROM
                intermediate_df
            WHERE
                "1099_Type" = 'INT' AND "1099_Amt" IS NOT NULL;
        """
        
        # Execute Query 2
        tables_for_query2 = {'intermediate_df': intermediate_df}
        final_report_df = self.execute_sql(QUERY_2_SQL, "Final Report Query (Query 2)", tables_for_query2)

        # Stop if the final query failed or returned no results
        if final_report_df is None or final_report_df.empty:
            self.log_message("WARNING: Final report query (Query 2) produced no results. Nothing to save.")
            messagebox.showwarning("No Results", "The final query produced no data to save.")
            return
            
        # 4. Save the final report (result of Query 2)
        self.save_final_report(final_report_df)

if __name__ == "__main__":
    root = tk.Tk()
    app = DataProcessorApp(root)
    root.mainloop()
