import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from pandasql import sqldf
import os
from datetime import datetime

# --- Constants for column names ---
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
        self.root.geometry("800x700")

        self.style = ttk.Style(self.root)
        self.style.theme_use("clam")

        self.tbl_file_path = tk.StringVar()
        self.tran_file_path = tk.StringVar()
        self.df_1099MTbl = None
        self.df_1099MTran = None
        
        # Determine the script's directory for automatic saving
        self.output_directory = os.path.dirname(os.path.abspath(__file__))

        self.create_widgets()

    def log_message(self, message):
        self.status_log.config(state=tk.NORMAL)
        self.status_log.insert(tk.END, message + "\n")
        self.status_log.see(tk.END)
        self.status_log.config(state=tk.DISABLED)
        self.root.update_idletasks()

    def create_widgets(self):
        """Create and layout all the GUI widgets."""
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Step 1: File Selection
        file_frame = ttk.LabelFrame(main_frame, text="Step 1: Upload Input Files", padding="10")
        file_frame.pack(fill=tk.X, padx=5, pady=5)
        file_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(file_frame, text="1099MTbl File (.dat):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tbl_file_path, state="readonly").grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tbl_file).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="1099MTran File (.dat):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tran_file_path, state="readonly").grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tran_file).grid(row=1, column=2, padx=5, pady=5)
        
        # Step 2: Load Data
        load_frame = ttk.LabelFrame(main_frame, text="Step 2: Load and Transform Data", padding="10")
        load_frame.pack(fill=tk.X, padx=5, pady=5)
        self.load_button = ttk.Button(load_frame, text="Load & Transform Files", command=self.run_load_and_transform)
        self.load_button.pack(pady=10, padx=20, fill=tk.X)
        
        # Step 3: Process and Save
        process_frame = ttk.LabelFrame(main_frame, text="Step 3: Generate Report", padding="10")
        process_frame.pack(fill=tk.X, padx=5, pady=5)
        self.validate_button = ttk.Button(process_frame, text="Save Transformed 1099MTran for Review (Optional)", command=self.save_tran_for_validation, state=tk.DISABLED)
        self.validate_button.pack(pady=(10, 5), padx=20, fill=tk.X)
        self.process_button = ttk.Button(process_frame, text="Generate Final Report", command=self.run_sql_processing, state=tk.DISABLED)
        self.process_button.pack(pady=(5, 10), padx=20, fill=tk.X)
        
        # Status Log
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
            self.validate_button.config(state=tk.DISABLED)
            self.process_button.config(state=tk.DISABLED)
            self.log_message(f"Selected {os.path.basename(file_path)} for {title}. Please load the data again.")

    def browse_tbl_file(self): self.browse_file(self.tbl_file_path, "1099MTbl")
    def browse_tran_file(self): self.browse_file(self.tran_file_path, "1099MTran")

    def run_load_and_transform(self):
        if not self.tbl_file_path.get() or not self.tran_file_path.get():
            messagebox.showerror("Input Error", "Please select both input files before loading.")
            return
        if self.load_and_transform_data():
            self.validate_button.config(state=tk.NORMAL)
            self.process_button.config(state=tk.NORMAL)
            self.log_message("-> SUCCESS: Data is loaded. Ready for validation or final processing.")
        else:
            self.validate_button.config(state=tk.DISABLED)
            self.process_button.config(state=tk.DISABLED)
            self.log_message("-> FAILED: Data loading failed.")

    def load_and_transform_data(self):
        try:
            self.log_message("--- Starting Data Loading and Transformation ---")
            self.df_1099MTbl = pd.read_csv(self.tbl_file_path.get(), sep='|', header=None, names=TBL_COLUMNS, dtype=str, skipinitialspace=True)
            self.df_1099MTran = pd.read_csv(self.tran_file_path.get(), sep='|', header=None, names=TRAN_COLUMNS, dtype=str, skipinitialspace=True)
            for col in ['Date_of_Transaction']: self.df_1099MTbl[col] = pd.to_datetime(self.df_1099MTbl[col], errors='coerce').dt.strftime('%m/%d/%Y')
            for col in ['Value_Date', 'Tran_Date']: self.df_1099MTran[col] = pd.to_datetime(self.df_1099MTran[col], errors='coerce').dt.strftime('%m/%d/%Y')
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

    def save_tran_for_validation(self):
        """Saves the transformed 1099MTran DataFrame automatically to the script's directory."""
        self.log_message("--- Validation Step: Saving transformed 1099MTran data ---")
        if self.df_1099MTran is None or self.df_1099MTran.empty:
            messagebox.showwarning("No Data", "1099MTran data is not available to save.")
            return

        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"Validation_1099MTran_{timestamp}.xlsx"
            save_path = os.path.join(self.output_directory, filename)
            
            self.df_1099MTran.to_excel(save_path, index=False, sheet_name="Transformed_1099MTran")
            
            self.log_message(f"-> SUCCESS: Validation file saved to:\n{save_path}")
            messagebox.showinfo("Success", f"Validation file has been saved automatically to:\n\n{save_path}")
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save the validation file.\n\nError: {e}")
            self.log_message(f"ERROR saving validation file: {e}")

    def execute_sql(self, query, query_name, tables):
        self.log_message(f"Executing {query_name}...")
        try:
            pysqldf = lambda q: sqldf(q, tables)
            result_df = pysqldf(query)
            self.log_message(f"-> Success: {query_name} returned {len(result_df)} rows.")
            return result_df
        except Exception as e:
            messagebox.showerror("SQL Query Error", f"The query '{query_name}' failed.\n\nError: {e}")
            self.log_message(f"ERROR executing {query_name}: {e}")
            return None

    def save_final_report(self, df):
        """Saves the final report DataFrame automatically to the script's directory."""
        self.log_message("Saving final report to Excel...")
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"Final_Report_{timestamp}.xlsx"
            save_path = os.path.join(self.output_directory, filename)

            df.to_excel(save_path, index=False, sheet_name="Final_Report")
            
            self.log_message(f"--- SUCCESS! Report saved to: {save_path} ---")
            messagebox.showinfo("Success", f"Report successfully generated and saved automatically to:\n\n{save_path}")
        except Exception as e:
            messagebox.showerror("Save Error", f"Failed to save the Excel file.\n\nError: {e}")
            self.log_message(f"ERROR saving Excel file: {e}")

    def run_sql_processing(self):
        """Orchestrates the two-step query process and saves the final result."""
        if self.df_1099MTran is None or self.df_1099MTbl is None:
            messagebox.showerror("Error", "Data is not loaded. Please use the 'Load & Transform Files' button first.")
            return
            
        # QUERY 1 (Intermediate)
        QUERY_1_SQL = "SELECT t1.*, t2.'1099_Type', t2.'1099_Amt' FROM df_1099MTran t1 LEFT JOIN df_1099MTbl t2 ON t1.ACID = t2.ACID;"
        tables_for_query1 = {'df_1099MTbl': self.df_1099MTbl, 'df_1099MTran': self.df_1099MTran}
        intermediate_df = self.execute_sql(QUERY_1_SQL, "Intermediate Query (Query 1)", tables_for_query1)

        if intermediate_df is None or intermediate_df.empty:
            self.log_message("WARNING: Intermediate query produced no results. Processing stopped.")
            messagebox.showwarning("No Results", "The first query produced no data.")
            return

        # QUERY 2 (Final)
        QUERY_2_SQL = "SELECT Loan_Number, Borrower_CIF, Tran_Date, Tran_Description, '1099_Type', '1099_Amt' FROM intermediate_df WHERE '1099_Type' = 'INT' AND '1099_Amt' IS NOT NULL;"
        tables_for_query2 = {'intermediate_df': intermediate_df}
        final_report_df = self.execute_sql(QUERY_2_SQL, "Final Report Query (Query 2)", tables_for_query2)

        if final_report_df is None or final_report_df.empty:
            self.log_message("WARNING: Final report query produced no results. Nothing to save.")
            messagebox.showwarning("No Results", "The final query produced no data to save.")
            return
            
        self.save_final_report(final_report_df)

if __name__ == "__main__":
    root = tk.Tk()
    app = DataProcessorApp(root)
    root.mainloop()
