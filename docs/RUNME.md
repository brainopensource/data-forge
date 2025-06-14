# Data Forge - Dynamic Data Platform


## Getting Started: Project Setup with `uv`

Follow these steps to set up your development environment from scratch.

### 1. Create your project directory and navigate into it

```bash
mkdir dynamic-data-platform
cd dynamic-data-platform
```

### 2. Steps for using `uv` to manage Python project dependencies

Follow this workflow:

1. **Create virtual environment**:  
   ```bash
   uv venv .venv
   ```

2. **Activate virtual environment**:  
   ```bash
   # Windows
   .venv\Scripts\activate
   
   # Unix/Linux/MacOS
   source .venv/bin/activate
   ```

3. **Initialize pyproject.toml**:  
   ```bash
   uv init
   ```

4. **Compile lock file**:  
   ```bash
   uv pip compile requirements.in -o requirements.lock
   ```
   Ensure `requirements.in` exists with your dependencies listed.

5. **Install dependencies**:  
   ```bash
   uv sync --locked
   ```

6. **Install top-level dependencies (optional)**:  
   ```bash
   uv pip install newpackage
   ```
   Use this only to add new dependencies.

7. **Export to requirements.txt (optional)**:  
   ```bash
   uv pip freeze > requirements.txt
   ```
   For legacy compatibility.

8. **Update lock file after adding packages**:  
   ```bash
   uv pip compile requirements.in -o requirements.lock
   ```

9. **Sync with lock file again if new package was added**:  
   ```bash
   uv sync --locked
   ```


### 3. Create a basic `.env` file

In your project's root directory, create a file named `.env` to hold your environment variables.

```env
# .env
APP_NAME="Dynamic Data Platform"
DEBUG_MODE=True
DUCKDB_PATH="data/database.duckdb"
```

### 4. Run your FastAPI application

Once all files are in place and dependencies are installed, you can start the FastAPI server.

```bash
uvicorn app.main:app --reload
```

On Windows, you can run the bat script:

```bash
.\app.bat
```

You can now access your API documentation at `http://127.0.0.1:8000/docs`.

---

### 5. Be Happy!
