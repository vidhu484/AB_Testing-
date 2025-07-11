import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import pandas as pd
from pandasql import sqldf
import os
from datetime import datetime

# --- The OFFICIAL Internal Schema ---
# The application will FORCE all loaded data to conform to this structure.
# This makes the SQL queries reliable, regardless of the input file's headers.
TBL_SCHEMA = {
    'name': '1099MTbl',
    'cols': [
        'ACID', '1099_Type', '1099_Amt', '1099_Source', 'Date_of_Transaction', 
        'Borrower_CIF', 'Cosigner_CIF'
    ],
    'count': 7
}

TRAN_SCHEMA = {
    'name': '1099MTran',
    'cols': [
        'ACID', 'Loan_Number', 'Borrower_CIF', 'Value_Date', 'UTC', 'Tran_Date', 
        'Tran_ID', 'Tran_Total', 'Tran_Prin', 'Tran_INT', 'Tran_Fee', 
        'Agent_ID_System_Processes_ID', 'Tran_Description', 'Tran_Remarks', 'Cosigner_CIF'
    ],
    'count': 15
}

def load_dat_file_sanitized(file_path, log_func):
    """
    The definitive parser. Reads raw bytes, removes null characters,
    skips the original header, and processes only the data rows.
    """
    log_func(f"-> Starting SANITIZED load for {os.path.basename(file_path)}...")
    try:
        with open(file_path, 'rb') as f:
            raw_content = f.read()

        sanitized_content_bytes = raw_content.replace(b'\x00', b'')
        
        if len(sanitized_content_bytes) < len(raw_content):
            log_func(f"  -> Found and removed null bytes from {os.path.basename(file_path)}.")

        decoded_content = sanitized_content_bytes.decode('latin1')
        lines = decoded_content.splitlines()

        # DISCARD the original header, we only care about data
        data_rows = lines[1:]
        
        if not data_rows:
            raise ValueError("File contains no data rows after the header.")

        # Determine expected field count from the header to validate rows
        header_line = lines[0]
        num_fields_expected = len(header_line.split('|'))
        if header_line.endswith('|'):
            num_fields_expected -= 1

        parsed_data = []
        for line in data_rows:
            if line.strip():
                fields = line.split('|')
                if fields and fields[-1] == '':
                    fields.pop()
                if len(fields) == num_fields_expected:
                    parsed_data.append(fields)
        
        df = pd.DataFrame(parsed_data, dtype=str)
        return df, len(df.columns)

    except Exception as e:
        raise type(e)(f"Critical failure parsing {os.path.basename(file_path)}: {e}")


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
        file_frame = ttk.LabelFrame(main_frame, text="Step 1: Upload Both Input Files", padding="10")
        file_frame.pack(fill=tk.X, padx=5, pady=5)
        file_frame.grid_columnconfigure(1, weight=1)
        ttk.Label(file_frame, text="File 1 (e.g., 1099MTbl):").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tbl_file_path, state="readonly").grid(row=0, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=lambda: self.browse_file(self.tbl_file_path)).grid(row=0, column=2, padx=5, pady=5)
        ttk.Label(file_frame, text="File 2 (e.g., 1099MTran):").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        ttk.Entry(file_frame, textvariable=self.tran_file_path, state="readonly").grid(row=1, column=1, padx=5, pady=5, sticky=tk.EW)
        ttk.Button(file_frame, text="Browse...", command=lambda: self.browse_file(self.tran_file_path)).grid(row=1, column=2, padx=5, pady=5)
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

    def browse_file(self, path_var):
        file_path = filedialog.askopenfilename(title="Select a DAT file", filetypes=(("Data Files", "*.dat"), ("All files", "*.*")))
        if file_path:
            path_var.set(file_path)
            self.validate_button.config(state=tk.DISABLED)
            self.process_button.config(state=tk.DISABLED)
            self.log_message(f"Selected {os.path.basename(file_path)}.")

    def run_load_and_transform(self):
        """The main intelligent loading and assignment logic."""
        if not self.tbl_file_path.get() or not self.tran_file_path.get():
            messagebox.showerror("Input Error", "Please select BOTH input files before loading.")
            return

        try:
            self.log_message("--- Starting Intelligent Loading Process ---")
            df1, count1 = load_dat_file_sanitized(self.tbl_file_path.get(), self.log_message)
            self.log_message(f"-> File 1 ({os.path.basename(self.tbl_file_path.get())}) has {count1} columns and {len(df1)} records.")
            
            df2, count2 = load_dat_file_sanitized(self.tran_file_path.get(), self.log_message)
            self.log_message(f"-> File 2 ({os.path.basename(self.tran_file_path.get())}) has {count2} columns and {len(df2)} records.")

            if count1 == TBL_SCHEMA['count'] and count2 == TRAN_SCHEMA['count']:
                self.df_1099MTbl = df1
                self.df_1099MTran = df2
            elif count1 == TRAN_SCHEMA['count'] and count2 == TBL_SCHEMA['count']:
                self.log_message("-> WARNING: Files appear to be swapped. Correcting automatically.")
                self.df_1099MTran = df1
                self.df_1099MTbl = df2
            else:
                raise ValueError(f"File column counts are incorrect. Please provide one file with {TBL_SCHEMA['count']} columns and one with {TRAN_SCHEMA['count']} columns.")

            # THE KEY STEP: Forcefully assign our standard, SQL-friendly column names
            self.df_1099MTbl.columns = TBL_SCHEMA['cols']
            self.df_1099MTran.columns = TRAN_SCHEMA['cols']
            self.log_message("-> Column names have been forcefully standardized.")

            # Apply Transformations
            self.log_message("Applying data transformations...")
            self.df_1099MTbl['Date_of_Transaction'] = pd.to_datetime(self.df_1099MTbl['Date_of_Transaction'], errors='coerce').dt.strftime('%m/%d/%Y')
            for col in ['Value_Date', 'Tran_Date']: self.df_1099MTran[col] = pd.to_datetime(self.df_1099MTran[col], errors='coerce').dt.strftime('%m/%d/%Y')
            self.df_1099MTbl['Borrower_CIF'] = self.df_1099MTbl['Borrower_CIF'].str.strip().str.zfill(10)
            self.df_1099MTbl['Cosigner_CIF'] = self.df_1099MTbl['Cosigner_CIF'].str.strip().str.zfill(10)
            self.df_1099MTran['Borrower_CIF'] = self.df_1099MTran['Borrower_CIF'].str.strip().str.zfill(10)
            self.df_1099MTran['Cosigner_CIF'] = self.df_1099MTran['Cosigner_CIF'].str.strip().str.zfill(10)
            self.df_1099MTran['Loan_Number'] = self.df_1099MTran['Loan_Number'].str.strip().str.zfill(15)
            self.log_message("-> Transformations complete.")
            
            self.validate_button.config(state=tk.NORMAL)
            self.process_button.config(state=tk.NORMAL)
            self.log_message(f"--- SUCCESS: Data is ready. 1099MTbl: {len(self.df_1099MTbl)} records. 1099MTran: {len(self.df_1099MTran)} records. ---")

        except Exception as e:
            messagebox.showerror("Loading Error", f"A critical error occurred:\n\n{e}")
            self.log_message(f"CRITICAL ERROR: {e}")
            self.validate_button.config(state=tk.DISABLED)
            self.process_button.config(state=tk.DISABLED)

    def save_tran_for_validation(self):
        # (This function is unchanged)
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
        # (This function is unchanged)
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
        # (This function is unchanged)
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
        # The SQL queries now work because the DataFrame columns match EXACTLY.
        if self.df_1099MTran is None or self.df_1099MTbl is None:
            messagebox.showerror("Error", "Data is not loaded. Please use the 'Load & Transform Files' button first.")
            return
        
        # NOTE: All column names in SQL must use the underscore format defined in the schemas.
        # Using double quotes is the safest way to ensure they work.
        QUERY_1_SQL = """
            SELECT 
                t1.*, 
                t2."1099_Type", 
                t2."1099_Amt" 
            FROM 
                df_1099MTran t1 
            LEFT JOIN 
                df_1099MTbl t2 ON t1.ACID = t2.ACID;
        """
        tables_for_query1 = {'df_1099MTbl': self.df_1099MTbl, 'df_1099MTran': self.df_1099MTran}
        intermediate_df = self.execute_sql(QUERY_1_SQL, "Intermediate Query (Query 1)", tables_for_query1)
        if intermediate_df is None or intermediate_df.empty:
            messagebox.showwarning("No Results", "The first query produced no data.")
            return
            
        QUERY_2_SQL = """
            SELECT 
                "Loan_Number", 
                "Borrower_CIF", 
                "Tran_Date", 
                "Tran_Description", 
                "1099_Type", 
                "1099_Amt" 
            FROM 
                intermediate_df 
            WHERE 
                "1099_Type" = 'INT' AND "1099_Amt" IS NOT NULL;
        """
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
