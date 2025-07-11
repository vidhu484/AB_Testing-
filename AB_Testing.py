###################
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from pandasql import sqldf
import os
from datetime import datetime

# --- Standard Column Names ---
TBL_COLUMNS = [
    'ACID', '1099_Type', '1099_Amt', '1099_Source', 'Date_of_Transaction', 
    'Borrower_CIF', 'Cosigner_CIF'
]
TRAN_COLUMNS = [
    'ACID', 'Loan_Number', 'Borrower_CIF', 'Value_Date', 'UTC', 'Tran_Date', 
    'Tran_ID', 'Tran_Total', 'Tran_Prin', 'Tran_INT', 'Tran_Fee', 
    'Agent_ID_System_Processes_ID', 'Tran_Description', 'Tran_Remarks', 'Cosigner_CIF'
]

def load_dat_file_final(file_path, expected_columns, log_func):
    """
    The definitive, robust DAT file parser. It reads the entire file content first,
    then splits by the actual newline character, making it immune to embedded newlines in data.
    """
    log_func(f"-> Starting robust load for {os.path.basename(file_path)}...")
    
    try:
        # Read the entire file content. Use 'latin1' for max compatibility.
        with open(file_path, 'r', encoding='latin1') as f:
            content = f.read()
            
        # Split the content into lines. `splitlines()` is robust to \n and \r\n.
        lines = content.splitlines()

        header_line = lines[0]
        data_lines = lines[1:]

        parsed_data = []
        malformed_lines = 0
        expected_field_count = len(expected_columns)

        for i, line in enumerate(data_lines):
            if not line.strip():
                continue # Skip empty lines

            fields = line.split('|')
            
            # Handle trailing delimiter: if the last element is empty, pop it.
            if fields and fields[-1] == '':
                fields.pop()

            # The most important validation step:
            if len(fields) == expected_field_count:
                parsed_data.append(fields)
            else:
                malformed_lines += 1
                # Log the first few errors to help diagnose if needed
                if malformed_lines <= 5:
                    log_func(f"  WARNING: Skipping malformed line #{i+2} in {os.path.basename(file_path)}. "
                             f"Expected {expected_field_count} fields, found {len(fields)}.")
        
        if malformed_lines > 0:
            log_func(f"-> Total malformed/skipped lines in {os.path.basename(file_path)}: {malformed_lines}")

        if not parsed_data:
            raise ValueError("No valid data rows could be parsed from the file.")

        # Create the DataFrame from the clean data
        df = pd.DataFrame(parsed_data, columns=expected_columns, dtype=str)
        return df

    except Exception as e:
        # Re-raise with a more informative message
        raise type(e)(f"Failed to parse file '{os.path.basename(file_path)}': {e}")


class DataProcessorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("1099 Data Processing Tool")
        self.root.geometry("800x700")
        self.style = ttk.Style(self.root)
        self.style.theme_use("clam")
        self.tbl_file_path = tk.StringVar()
        self.tran_file_path = tk.StringVar()
        self.df_1099MTbl = None
        self.df_1099MTran = None
        try: self.output_directory = os.path.dirname(os.path.abspath(__file__))
        except NameError: self.output_directory = os.getcwd()
        self.create_widgets()

    def log_message(self, message):
        self.status_log.config(state=tk.NORMAL)
        self.status_log.insert(tk.END, message + "\n")
        self.status_log.see(tk.END)
        self.status_log.config(state=tk.DISABLED)
        self.root.update_idletasks()
        
    def create_widgets(self):
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        file_frame = ttk.LabelFrame(main_frame, text="Step 1: Upload Input Files", padding="10")
        file_frame.pack(fill=tk.X, padx=5, pady=5)
        file_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(file_frame, text="1099MTbl File (.dat):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tbl_file_path, state="readonly").grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tbl_file).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="1099MTran File (.dat):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tran_file_path, state="readonly").grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=self.browse_tran_file).grid(row=1, column=2, padx=5, pady=5)
        load_frame = ttk.LabelFrame(main_frame, text="Step 2: Load and Transform Data", padding="10")
        load_frame.pack(fill=tk.X, padx=5, pady=5)
        self.load_button = ttk.Button(load_frame, text="Load & Transform Files", command=self.run_load_and_transform)
        self.load_button.pack(pady=10, padx=20, fill=tk.X)
        process_frame = ttk.LabelFrame(main_frame, text="Step 3: Generate Report", padding="10")
        process_frame.pack(fill=tk.X, padx=5, pady=5)
        self.validate_button = ttk.Button(process_frame, text="Save Transformed 1099MTran for Review (Optional)", command=self.save_tran_for_validation, state=tk.DISABLED)
        self.validate_button.pack(pady=(10, 5), padx=20, fill=tk.X)
        self.process_button = ttk.Button(process_frame, text="Generate Final Report", command=self.run_sql_processing, state=tk.DISABLED)
        self.process_button.pack(pady=(5, 10), padx=20, fill=tk.X)
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
            self.log_message(f"Selected {os.path.basename(file_path)}. Please load the data.")

    def browse_tbl_file(self): self.browse_file(self.tbl_file_path, "1099MTbl")
    def browse_tran_file(self): self.browse_file(self.tran_file_path, "1099MTran")

    def run_load_and_transform(self):
        if not self.tbl_file_path.get() or not self.tran_file_path.get():
            messagebox.showerror("Input Error", "Please select both input files before loading.")
            return
        if self.load_and_transform_data():
            self.validate_button.config(state=tk.NORMAL)
            self.process_button.config(state=tk.NORMAL)
            self.log_message(f"-> SUCCESS: Data loaded. 1099MTbl: {len(self.df_1099MTbl)} records. 1099MTran: {len(self.df_1099MTran)} records.")
        else:
            self.validate_button.config(state=tk.DISABLED)
            self.process_button.config(state=tk.DISABLED)
            self.log_message("-> FAILED: Data loading failed. Check logs for details.")

    def load_and_transform_data(self):
        """Loads data using the definitive parser and then applies transformations."""
        try:
            self.log_message("--- Starting Final Robust Data Loading ---")
            
            # Use the new, definitive parser function
            self.df_1099MTbl = load_dat_file_final(self.tbl_file_path.get(), TBL_COLUMNS, self.log_message)
            self.df_1099MTran = load_dat_file_final(self.tran_file_path.get(), TRAN_COLUMNS, self.log_message)
            
            # --- Apply Transformations ---
            self.log_message("Applying data transformations...")
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
            messagebox.showerror("Loading Error", f"A critical error occurred during file loading.\n\nError: {e}")
            self.log_message(f"CRITICAL ERROR: {e}")
            return False

    # The rest of the functions (save, execute_sql, run_sql_processing) are unchanged
    # and will now work correctly with the properly loaded DataFrames.
    def save_tran_for_validation(self):
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

    def execute_sql(self, query, query_name, tables):
        self.log_message(f"Executing {query_name}...")
        try:
            pysqldf = lambda q: sqldf(q, tables)
            result_df = pysqldf(query)
            self.log_message(f"-> Success: {query_name} returned {len(result_df)} rows.")
            return result_df
        except Exception as e:
            error_msg = f"The query '{query_name}' failed.\n\nError: {e}"
            messagebox.showerror("SQL Query Error", error_msg)
            self.log_message(f"ERROR executing {query_name}: {e}")
            return None

    def save_final_report(self, df):
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
            
    def run_sql_processing(self):
        if self.df_1099MTran is None or self.df_1099MTbl is None:
            messagebox.showerror("Error", "Data is not loaded. Please use the 'Load & Transform Files' button first.")
            return
        QUERY_1_SQL = """SELECT t1.*, t2."1099_Type", t2."1099_Amt" FROM df_1099MTran t1 LEFT JOIN df_1099MTbl t2 ON t1.ACID = t2.ACID;"""
        tables_for_query1 = {'df_1099MTbl': self.df_1099MTbl, 'df_1099MTran': self.df_1099MTran}
        intermediate_df = self.execute_sql(QUERY_1_SQL, "Intermediate Query (Query 1)", tables_for_query1)
        if intermediate_df is None or intermediate_df.empty:
            messagebox.showwarning("No Results", "The first query produced no data.")
            return
        QUERY_2_SQL = """SELECT Loan_Number, Borrower_CIF, Tran_Date, Tran_Description, "1099_Type", "1099_Amt" FROM intermediate_df WHERE "1099_Type" = 'INT' AND "1099_Amt" IS NOT NULL;"""
        tables_for_query2 = {'intermediate_df': intermediate_df}
        final_report_df = self.execute_sql(QUERY_2_SQL, "Final Report Query (Query 2)", tables_for_query2)
        if final_report_df is None or final_report_df.empty:
            messagebox.showwarning("No Results", "The final query produced no data to save.")
            return
        self.save_final_report(final_report_df)

if __name__ == "__main__":
    root = tk.Tk()
    app = DataProcessorApp(root)
    root.mainloop()
