# Frontend README

A modern, professional desktop application for data management, exploration, and visualization built with Python and CustomTkinter.

## 🚀 Features

### Core Functionality

- **Data Upload/Download** - Upload CSV data to server and download in various formats
- **Schema Management** - Register, view, and manage data schemas
- **External Data Fetching** - Fetch data from external APIs (OData, CSV, HTML)
- **Advanced Data Exploration** - Interactive table explorer with filtering and pagination
- **Visual Analytics** - Interactive plotting with matplotlib integration
- **Export Capabilities** - Export data to CSV, JSON, and Arrow formats

### Architecture Highlights

- **Clean Architecture** - Layered design with separation of concerns
- **Dependency Injection** - Professional service resolution via container
- **CQRS Pattern** - Command/Query separation for data operations
- **Error Handling** - Comprehensive error management with logging
- **UI Framework Abstraction** - Supports both CustomTkinter and fallback tkinter

## 📋 Requirements

### Python Dependencies

```bash
# Core dependencies
customtkinter>=5.0.0
tkinter (built-in)
requests>=2.28.0
matplotlib>=3.6.0

# Optional (enhanced functionality)
pyarrow>=10.0.0  # For Arrow IPC data format
polars>=0.19.0   # For enhanced CSV export
pandas>=1.5.0    # For correlation plots
plotly>=5.0.0    # For advanced interactive plots (Task 12)
```

### System Requirements

- Python 3.8+
- Windows 10+ (primary target)
- 4GB RAM minimum
- Internet connection for external data fetching

## 🛠️ Installation

### 1. Clone Repository

```bash
git clone <repository-url>
cd react-fast-V13
```

### 2. Install Dependencies

```bash
pip install customtkinter requests matplotlib
# Optional enhanced features
pip install pyarrow polars pandas plotly
```

### 3. Backend Setup

Ensure the DataForge backend is running on `http://localhost:8080`

### 4. Run Application

```bash
# Method 1: Direct execution
python frontend/app.py

# Method 2: Module execution
python -m frontend.main

# Method 3: Batch file (Windows)
DataForge_Front.bat
```

## 📁 Project Structure

```
frontend/
├── app.py                      # Main application entry point (6800+ lines)
├── main.py                     # Alternative entry point
├── __init__.py
│
├── application/                # Application layer (CQRS)
│   ├── commands/              # Command handlers
│   ├── handlers/              # Business logic handlers
│   └── queries/               # Query handlers
│
├── components/                # Reusable UI components
│   ├── base_component.py      # Base component class
│   ├── data_explorer.py       # Table exploration component
│   ├── plot_explorer.py       # Plotting component (matplotlib)
│   └── plotly_explorer.py     # Advanced plotting (Plotly) - Task 12
│
├── controllers/               # MVC controllers
│   ├── main_window_controller.py  # Window management
│   ├── navigation_controller.py   # Navigation logic
│   ├── data_controller.py         # Data operations
│   └── ui_controller.py           # UI state management
│
├── core/                      # Core infrastructure
│   ├── container.py           # Dependency injection
│   ├── application_service.py # Application services
│   ├── plugin_system.py       # Plugin management (Task 14)
│   └── interfaces/            # Interface definitions
│       ├── icontroller.py
│       ├── irepository.py
│       ├── iview.py
│       └── plugin_interfaces.py  # Plugin interfaces
│
├── domain/                    # Domain layer
│   ├── entities/              # Domain entities
│   │   ├── data_record.py
│   │   ├── plot_config.py     # Plot configuration (Task 12)
│   │   └── validation_rules.py # Validation rules (Task 11)
│   └── services/              # Domain services
│       └── data_validation_service.py  # Data validation (Task 11)
│
├── presentation/              # Presentation layer
│   ├── components/            # UI components
│   │   ├── enhanced_components.py    # Enhanced UI components (Task 13)
│   │   ├── data_card.py             # Data card component
│   │   ├── navigation_sidebar.py    # Navigation sidebar
│   │   └── status_bar.py            # Status bar component
│   ├── layouts/               # Layout management
│   │   └── responsive_layout_manager.py  # Responsive layouts (Task 13)
│   └── styles/                # Theming and styles
│       ├── theme.py          # Color theme definition
│       └── button_factory.py # UI element factory
│
├── services/                  # Infrastructure services
│   ├── api_client.py         # Backend API client
│   ├── data_generator.py     # Sample data generation
│   ├── ui_framework_adapter.py           # UI framework abstraction
│   ├── enhanced_ui_framework_adapter.py  # Enhanced UI framework (Task 13)
│   ├── data_cleaning_service.py          # Data cleaning (Task 11)
│   └── websocket_client.py               # WebSocket client (Future)
│
├── tabs/                      # Tab implementations
│   ├── home_tab.py           # Home tab
│   ├── database_tab.py       # Database operations
│   ├── exploration_tab.py    # Data exploration
│   │   ├── table_explorer    # Table view subtab
│   │   ├── visual_analytics  # Matplotlib plots subtab
│   │   └── more_plots        # Plotly plots subtab (Task 12)
│   ├── gateway_tab.py        # Feature gateway
│   ├── help_tab.py           # Help and about
│   ├── logs_tab.py           # Application logs
│   └── plugins_tab.py        # Plugin management (Task 14)
│
└── utils/                     # Utility modules
    ├── string_utils.py        # String manipulation
    ├── error_handler.py       # Error handling
    ├── data_type_detector.py  # Data type detection
    ├── async_runner.py        # Async operations
    ├── config.py             # Configuration management
    └── ui_helpers.py         # UI utility functions
```

## 🎯 Usage Guide

### 1. Home Tab

- Overview of application features
- Quick navigation to main functions

### 2. Database Operations

- **Upload**: Generate sample data and upload to server
- **Download**: Read data from server with format options
- **Schema Management**: View and register schemas

### 3. External Fetch

- **Generic HTTP**: Download HTML/text content
- **CSV Direct**: Download CSV files
- **OData API**: Fetch from OData endpoints with pagination
- **Authentication**: Basic auth support

### 4. Data Exploration

- **Table Explorer**: 
  - Interactive pagination (optimized for large datasets)
  - Advanced filtering (contains, equals, greater/less than)
  - Column sorting and data type detection
  - Export filtered results
  - Data validation and cleaning (Task 11)
  
- **Visual Analytics**:
  - Multiple plot types (scatter, line, bar, histogram, box)
  - Individual and group plotting modes
  - Data filtering and aggregation
  - Interactive matplotlib integration
  
- **More Plots** (Task 12):
  - Advanced interactive plots with Plotly
  - Statistical visualizations (violin, density, treemap)
  - Export to HTML, PNG, SVG formats
  - Professional presentation-ready charts

### 5. Plugin Management (Task 14)

- Discover and install plugins
- Load/unload plugins dynamically
- Configure plugin settings
- Custom data sources and visualizations

## 🔧 Configuration

### API Configuration

```python
# frontend/app.py
class AppConfig:
    API_BASE_URL = "http://localhost:8080"
    DEFAULT_SCHEMA = "well_production"
    DEFAULT_RECORDS = "10000"
    DEFAULT_COMPRESSION = "zstd"
```

### Theme Configuration

```python
# frontend/presentation/styles/theme.py
class Theme:
    COLOR_PRIMARY = "#1f538d"      # Blue
    COLOR_SECONDARY = "#7b1fa2"    # Purple
    COLOR_SURFACE_LIGHT = "#404040" # Dark gray
    COLOR_TEXT_PRIMARY = "#ffffff"  # White
```

## 🏗️ Architecture Overview

### Clean Architecture Layers

1. **Domain Layer** - Core business entities and rules
2. **Application Layer** - Use cases and business logic (CQRS)
3. **Infrastructure Layer** - External concerns (API, UI, storage)
4. **Presentation Layer** - UI components and user interactions

### Key Design Patterns

- **CQRS** - Command Query Responsibility Segregation
- **Dependency Injection** - Service resolution via container
- **Observer Pattern** - UI state management
- **Factory Pattern** - UI element creation
- **Repository Pattern** - Data access abstraction
- **Plugin Architecture** - Extensible modular design (Task 14)

### Error Handling Strategy

- **Centralized Error Handler** - Consistent error processing
- **Logging Integration** - File and console logging
- **User-Friendly Messages** - Technical errors converted to user messages
- **Graceful Degradation** - Fallbacks for missing dependencies

## 🚀 Performance Features

### Data Handling

- **Lazy Loading** - Load data on demand for large datasets
- **Pagination** - Efficient table rendering (50-1000 rows per page)
- **Search Indexing** - Fast text search across columns
- **Memory Management** - Configurable memory limits
- **Data Validation** - Automated quality checks (Task 11)

### UI Optimization

- **Responsive Design** - Non-blocking UI with async operations
- **Progressive Loading** - Background data fetching
- **Caching** - Column statistics and data type caching
- **Debouncing** - Optimized search and filter operations
- **Component Reusability** - Enhanced UI framework (Task 13)

## 🧪 Testing

### Manual Testing Checklist

- [ ] Application startup and navigation
- [ ] Data upload/download operations
- [ ] Schema management functions
- [ ] External data fetching (all types)
- [ ] Table exploration with large datasets
- [ ] Visual analytics plotting (matplotlib)
- [ ] Advanced plotting (Plotly - Task 12)
- [ ] Data validation and cleaning (Task 11)
- [ ] Plugin management (Task 14)
- [ ] Responsive UI behavior (Task 13)
- [ ] Export functionality
- [ ] Error handling scenarios

### Test Data

- Sample well production data generation
- CSV file upload testing
- OData API endpoint testing
- Plugin functionality testing

## 🐛 Troubleshooting

### Common Issues

**PyArrow Missing**
```
Error: Cannot read data for exploration. PyArrow is required.
Solution: pip install pyarrow
```

**CustomTkinter Issues**
```
Error: CustomTkinter import failed
Solution: App automatically falls back to tkinter
```

**Plotly Missing (Task 12)**
```
Error: Interactive plots require Plotly
Solution: pip install plotly
```

**API Connection Failed**
```
Error: Connection refused
Solution: Ensure backend is running on http://localhost:8080
```

**Large Dataset Performance**
```
Issue: Slow table loading
Solution: Use smaller page sizes (25-50 rows) in data explorer
```

**Plugin Loading Issues (Task 14)**
```
Error: Plugin failed to load
Solution: Check plugin dependencies and manifest format
```

## 🔮 Development Roadmap

### Completed Tasks (1-10)
- ✅ String utilities and error handling
- ✅ Data type detection and validation
- ✅ MVC controllers and UI management
- ✅ Component system and framework adapter
- ✅ Integration foundation and dependency injection

### Planned Enhancements (11-14)
- 🔄 **Task 11**: Enhanced data validation and cleaning service
- 🔄 **Task 12**: Advanced visualization engine with Plotly
- 🔄 **Task 13**: Enhanced UI framework and responsive design
- 🔄 **Task 14**: Plugin architecture and extensibility

### Future Considerations
- Real-time data streaming and live updates
- Advanced analytics and machine learning integration
- Cloud deployment and scaling capabilities
- Mobile companion application

## 👨‍💻 Development

### Code Style

- **PEP 8** compliance
- **Type hints** for better code documentation
- **Docstrings** for all public methods
- **Error handling** at all integration points
- **Clean Architecture** principles

### Contributing

1. Follow clean architecture principles
2. Add comprehensive error handling
3. Include type hints and docstrings
4. Test on both CustomTkinter and tkinter fallback
5. Update documentation for new features
6. Consider plugin architecture for extensions

### Plugin Development (Task 14)

Create custom plugins by implementing the appropriate interface:
- `IDataSourcePlugin` for custom data sources
- `IVisualizationPlugin` for custom plot types
- `IWorkflowPlugin` for automation workflows
- `IExportPlugin` for custom export formats

## 📄 License

[Your License Here]

## 🆘 Support

For issues and questions:
- Check troubleshooting section
- Review application logs
- Check plugin compatibility (Task 14)
- Create GitHub issue with error details and system information
