# Enhanced Plotting Features - Individual & Group Plots

## Overview

The DataForge frontend now includes two powerful plotting modes that address real-world data analysis needs:

1. **👤 Individual Plots** - Filtered plotting for focused analysis
2. **👥 Group Plots** - Aggregated plotting for comparative analysis

## Features

### 🎯 Individual Plots Mode

**Purpose**: Create focused plots on filtered subsets of your data, perfect for time series and specific field analysis.

**Key Features**:
- **Data Filtering**: Filter by any column before plotting
- **Time Series Ready**: Perfect for analyzing single field/well over time
- **Interactive Filtering**: Real-time filter application with instant feedback
- **All Plot Types**: Supports scatter, line, bar, histogram, box, and correlation plots

**Example Use Cases**:
- Plot oil production over time for a specific field
- Analyze pressure trends for a particular well
- Compare production metrics for a single asset

**How to Use**:
1. Select "👤 Individual Plots" mode
2. Choose a filter column (optional - use "None" for all data)
3. Select a specific filter value (e.g., "Field_A")
4. Click "🔍 Apply Filter"
5. Select X and Y axis columns
6. Choose plot type and generate

### 🔄 Group Plots Mode

**Purpose**: Create comparative plots showing aggregated data across multiple groups, ideal for comparing different fields, wells, or categories.

**Key Features**:
- **Group Selection**: Choose which groups to include in comparison
- **Multiple Aggregation Functions**: mean, sum, count, min, max, median
- **Multi-Series Plots**: Each group appears as a separate series with distinct colors
- **Interactive Group Selection**: Dialog-based group picker with select all/none options

**Aggregation Functions**:
- **Mean**: Average values within each group
- **Sum**: Total values within each group
- **Count**: Number of records in each group
- **Min/Max**: Minimum/Maximum values within each group
- **Median**: Middle values within each group

**Example Use Cases**:
- Compare average production across multiple fields
- Show total production trends for different regions
- Analyze production variance between well types

**How to Use**:
1. Select "👥 Group Plots" mode
2. Choose a "Group By Column" (e.g., "field_name")
3. Select aggregation function (e.g., "mean")
4. Click "📋 Select Groups" to choose which groups to compare
5. Select X and Y axis columns
6. Choose plot type and generate

## Technical Implementation

### Data Flow

```
Original Data → Filter/Group → Aggregate → Plot Generation
```

**Individual Mode**:
```python
data → apply_filter(column, value) → filtered_data → plot
```

**Group Mode**:
```python
data → group_by(column) → aggregate(function) → grouped_data → multi_series_plot
```

### Supported Plot Types

#### Individual Plots
- **Scatter Plot**: Relationship analysis with filtered data
- **Line Plot**: Time series for specific entities
- **Bar Plot**: Category comparisons within filtered subset
- **Histogram**: Distribution analysis of filtered data
- **Box Plot**: Statistical distribution of filtered values
- **Correlation**: Correlation matrix of filtered dataset

#### Group Plots
- **Scatter Plot**: Multi-group scatter with different colors
- **Line Plot**: Overlapping time series for group comparison
- **Bar Plot**: Side-by-side bars for group comparison
- **Box Plot**: Distribution comparison across groups

### Color Coding

Group plots automatically assign distinct colors to each group:
- Group 1: Blue (#1f77b4)
- Group 2: Orange (#ff7f0e)
- Group 3: Green (#2ca02c)
- Group 4: Red (#d62728)
- Group 5: Purple (#9467bd)
- And more...

## User Interface

### Mode Selection
- Toggle buttons at the top: "👤 Individual Plots" / "👥 Group Plots"
- Active mode highlighted with blue background
- Configuration panel changes based on selected mode

### Individual Mode UI
```
🔍 Data Filter (Optional)
├── Filter Column: [Dropdown]
├── Filter Value: [Dropdown with unique values]
├── [🔍 Apply Filter] Button
└── Status: "📊 X records after filter"

📊 Plot Configuration
├── X-Axis Column: [Dropdown]
├── Y-Axis Column: [Dropdown]
├── Plot Type: [Dropdown]
└── [📊 Generate Plot] [🗑️ Clear]
```

### Group Mode UI
```
👥 Group Configuration
├── Group By Column: [Dropdown]
├── Aggregation: [mean/sum/count/min/max/median]
├── [📋 Select Groups] Button
└── Status: "📊 X groups selected"

📊 Plot Configuration
├── X-Axis Column: [Dropdown]
├── Y-Axis Column: [Dropdown]
├── Plot Type: [Dropdown]
└── [📊 Generate Plot] [🗑️ Clear]
```

## Practical Examples

### Example 1: Oil Production Time Series for Specific Field

**Mode**: Individual Plots
**Filter**: field_name = "Field_A"
**X-Axis**: production_date
**Y-Axis**: oil_production_kbd
**Plot Type**: Line

Result: Shows oil production trend over time for Field_A only.

### Example 2: Compare Average Production Across Fields

**Mode**: Group Plots
**Group By**: field_name
**Aggregation**: mean
**Selected Groups**: Field_A, Field_B, Field_C
**X-Axis**: production_date
**Y-Axis**: oil_production_kbd
**Plot Type**: Line

Result: Shows three overlapping lines comparing average oil production trends for the three selected fields.

### Example 3: Well Performance Distribution by Field

**Mode**: Group Plots
**Group By**: field_name
**Aggregation**: mean
**Selected Groups**: All fields
**X-Axis**: field_name
**Y-Axis**: oil_production_kbd
**Plot Type**: Box

Result: Shows box plots comparing oil production distributions across all fields.

## Benefits

### For Individual Plots
✅ **Focused Analysis**: Analyze specific subsets without noise
✅ **Time Series Ready**: Perfect for temporal analysis
✅ **Clean Visualizations**: No overlapping data from different entities
✅ **Interactive Filtering**: Real-time data subset selection

### For Group Plots
✅ **Comparative Analysis**: Direct comparison between groups
✅ **Statistical Aggregation**: Choose appropriate aggregation method
✅ **Multi-Series Visualization**: All groups on same chart for easy comparison
✅ **Flexible Grouping**: Group by any categorical column

## Performance Considerations

- **Individual Plots**: Efficient filtering reduces plot data size
- **Group Plots**: Aggregation reduces data points for better performance
- **Interactive Updates**: Real-time feedback on data size changes
- **Memory Management**: Only selected groups loaded into plot data

## Future Enhancements

Planned improvements:
- **Date Range Filtering**: Time-based filtering for time series
- **Multiple Group Columns**: Group by combinations of columns
- **Custom Aggregation**: User-defined aggregation functions
- **Export Group Data**: Save aggregated data as CSV/JSON
- **Advanced Statistics**: Trend lines, confidence intervals for groups

This enhanced plotting system transforms the DataForge frontend into a powerful data analysis tool, enabling both detailed examination of specific data subsets and high-level comparative analysis across different groups.
