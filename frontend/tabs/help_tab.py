"""
Help Tab for DataForge
"""
try:
    import customtkinter as ctk
    HAS_CTK = True
except ImportError:
    HAS_CTK = False

class HelpTab:
    def __init__(self, parent, app):
        self.parent = parent
        self.app = app
        self.main_frame = ctk.CTkFrame(self.parent, fg_color="transparent")
        self.build()

    def build(self):
        """Builds the Help tab UI."""
        help_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        help_frame.pack(fill="both", expand=True, padx=20, pady=20)

        title_label = ctk.CTkLabel(help_frame, text="Help & Documentation", font=ctk.CTkFont(size=20, weight="bold"))
        title_label.pack(pady=(0, 20))

        help_text = """
        **Welcome to the DataForge Help Section!**

        Here you can find information about the different features of the application.

        **1. Database Tab**
        - **Upload:** Generate synthetic data based on a schema and write it to the database. You can specify the number of records and the compression type.
        - **Download:** Read data from a specified schema in the database. You can also directly save the data to a CSV file.
        - **Schema Management:** List all available schemas, register new ones (e.g., the example 'user_profile' schema), and view the details and version history of existing schemas.

        **2. Exploration Tab**
        This is your main workspace for data analysis.
        - **Load Data:** You can load data from the current database schema, use pre-generated sample data, or upload your own CSV file.
        - **Table Explorer:** An advanced data grid with features like:
            - **Pagination:** Efficiently handles large datasets.
            - **Sorting:** Click on column headers to sort.
            - **Quick Search:** Instantly search across all columns.
            - **Advanced Filtering:** Build complex filter queries.
            - **Export:** Export the filtered data to a CSV file.
        - **Plot Explorer:** A powerful tool to visualize your data.
            - **Plot Types:** Supports scatter, line, bar, histogram, and box plots.
            - **Individual vs. Group Mode:** Create plots from raw data points or aggregated group data.
            - **Interactive Filtering:** Select data subsets to plot.
            - **Customization:** Choose columns for X/Y axes and specify data types.

        **3. Gateway Tab**
        Fetch data from external web sources.
        - **URL Input:** Enter the URL of a REST API, OData endpoint, or a direct link to a CSV file.
        - **Fetch Type:** Specify the expected data format (JSON, OData, CSV) to ensure correct parsing.
        - **Save/Load:** Once fetched, you can save the raw data to a local file or directly load it into the Exploration tab for analysis.

        **General Tips:**
        - The **log panel** at the bottom of the window shows a detailed history of all operations.
        - The **status bar** provides quick feedback on the last action.
        - Long-running operations (like network requests) run in the background to keep the UI responsive. The **progress bar** will indicate their status.
        """

        # Using a Textbox for more control over formatting if needed in the future
        help_textbox = ctk.CTkTextbox(help_frame, wrap="word")
        help_textbox.pack(fill="both", expand=True)
        help_textbox.insert("1.0", help_text)
        help_textbox.configure(state="disabled") # Make it read-only
