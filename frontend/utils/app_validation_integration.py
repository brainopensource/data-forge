"""
Example integration of validation service into the main DataForge app.

This demonstrates how to add professional data validation features
using CQRS patterns and dependency injection.
"""

from frontend.core.container import configure_services, get_service
from frontend.utils.validation_integration import validation_integration
from frontend.utils.validation_popups import ValidationResultPopup
from frontend.services.ui_framework_adapter import UIFrameworkAdapter


def add_validation_features_to_app(app_instance):
    """
    Add validation features to an existing DataForge app instance.
    
    This function demonstrates how to integrate the new validation service
    into the existing monolithic app structure, making it more modular.
    
    Args:
        app_instance: The main DataForge app instance
    """
    
    # Initialize the DI container
    configure_services()
    
    # Get UI adapter
    ui_adapter = get_service(UIFrameworkAdapter)
    
    # Create validation popup helper
    validation_popup = ValidationResultPopup(app_instance, ui_adapter)
    
    # Add validation methods to the app instance
    def validate_current_data():
        """Validate the currently loaded data in the app."""
        try:
            # Get current data from app (adjust based on actual app structure)
            current_data = getattr(app_instance, 'current_data', None)
            if not current_data:
                app_instance._log("❌ No data loaded to validate")
                return
            
            app_instance._log("🔍 Running data validation...")
            
            # Run validation
            result = validation_integration.validate_data_sync(current_data)
            
            # Show results
            validation_popup.show_validation_results(result)
            
            # Log summary
            if result["success"]:
                app_instance._log(f"✅ {result['message']}")
            else:
                app_instance._log(f"❌ {result['message']}")
                
        except Exception as e:
            app_instance._log(f"❌ Validation failed: {str(e)}")
    
    def clean_current_data():
        """Clean the currently loaded data in the app."""
        try:
            current_data = getattr(app_instance, 'current_data', None)
            if not current_data:
                app_instance._log("❌ No data loaded to clean")
                return
            
            app_instance._log("🧹 Cleaning data...")
            
            # Run cleaning
            result = validation_integration.clean_data_sync(current_data)
            
            # Show results
            validation_popup.show_cleaning_results(result)
            
            # Update app data if cleaning was successful
            if result["success"] and result.get("cleaned_data"):
                setattr(app_instance, 'current_data', result["cleaned_data"])
                app_instance._log(f"✅ {result['message']}")
            else:
                app_instance._log(f"❌ {result['message']}")
                
        except Exception as e:
            app_instance._log(f"❌ Data cleaning failed: {str(e)}")
    
    def show_quality_report():
        """Show data quality report for current data."""
        try:
            current_data = getattr(app_instance, 'current_data', None)
            if not current_data:
                app_instance._log("❌ No data loaded to analyze")
                return
            
            app_instance._log("📊 Generating quality report...")
            
            # Generate report
            result = validation_integration.get_quality_report_sync(current_data)
            
            # Show results
            validation_popup.show_quality_report(result)
            
            if result["success"]:
                app_instance._log(f"✅ {result['message']}")
            else:
                app_instance._log(f"❌ {result['message']}")
                
        except Exception as e:
            app_instance._log(f"❌ Quality report failed: {str(e)}")
    
    def show_recommendations():
        """Show data quality recommendations."""
        try:
            current_data = getattr(app_instance, 'current_data', None)
            if not current_data:
                app_instance._log("❌ No data loaded to analyze")
                return
            
            app_instance._log("💡 Generating recommendations...")
            
            # Get recommendations
            result = validation_integration.get_recommendations_sync(current_data)
            
            # Show results
            validation_popup.show_recommendations(result)
            
            if result["success"]:
                app_instance._log(f"✅ {result['message']}")
            else:
                app_instance._log(f"❌ {result['message']}")
                
        except Exception as e:
            app_instance._log(f"❌ Recommendations failed: {str(e)}")
    
    def validate_and_clean():
        """Validate and clean data in one operation."""
        try:
            current_data = getattr(app_instance, 'current_data', None)
            if not current_data:
                app_instance._log("❌ No data loaded to process")
                return
            
            app_instance._log("🔍🧹 Running validation and cleaning...")
            
            # Run async validation and cleaning
            def on_complete(result):
                try:
                    # Update UI on main thread
                    app_instance.after(0, lambda: _handle_validation_cleaning_result(result))
                except:
                    pass  # Handle case where after method is not available
            
            validation_integration.validate_and_clean_async(current_data, on_complete)
            
        except Exception as e:
            app_instance._log(f"❌ Validation and cleaning failed: {str(e)}")
    
    def _handle_validation_cleaning_result(result):
        """Handle the result of validation and cleaning operation."""
        try:
            # Show results
            validation_popup.show_validation_and_cleaning_results(result)
            
            # Update app data if cleaning was successful
            if result.get("success") and result.get("cleaned_data"):
                setattr(app_instance, 'current_data', result["cleaned_data"])
            
            # Log result
            if result.get("success"):
                app_instance._log(f"✅ {result.get('message', 'Validation and cleaning completed')}")
            else:
                app_instance._log(f"❌ {result.get('message', 'Validation and cleaning failed')}")
                
        except Exception as e:
            app_instance._log(f"❌ Error handling validation result: {str(e)}")
    
    # Attach methods to app instance
    app_instance.validate_current_data = validate_current_data
    app_instance.clean_current_data = clean_current_data
    app_instance.show_quality_report = show_quality_report
    app_instance.show_recommendations = show_recommendations
    app_instance.validate_and_clean = validate_and_clean
    
    # Return a dictionary of the new methods for easy access
    return {
        'validate_data': validate_current_data,
        'clean_data': clean_current_data,
        'quality_report': show_quality_report,
        'recommendations': show_recommendations,
        'validate_and_clean': validate_and_clean
    }


def add_validation_buttons_to_sidebar(app_instance, sidebar_frame):
    """
    Add validation buttons to the app sidebar.
    
    Args:
        app_instance: The main app instance
        sidebar_frame: The sidebar frame to add buttons to
    """
    
    # Get UI adapter
    ui_adapter = get_service(UIFrameworkAdapter)
    
    # Add validation section header
    validation_header = ui_adapter.create_label(
        sidebar_frame,
        text="🔍 Data Quality",
        font=("Arial", 12, "bold")
    )
    validation_header.pack(pady=(20, 10))
    
    # Validate button
    validate_btn = ui_adapter.create_button(
        sidebar_frame,
        text="🔍 Validate Data",
        command=lambda: app_instance.validate_current_data() if hasattr(app_instance, 'validate_current_data') else None
    )
    validate_btn.pack(pady=2, padx=20, fill="x")
    
    # Clean button
    clean_btn = ui_adapter.create_button(
        sidebar_frame,
        text="🧹 Clean Data", 
        command=lambda: app_instance.clean_current_data() if hasattr(app_instance, 'clean_current_data') else None
    )
    clean_btn.pack(pady=2, padx=20, fill="x")
    
    # Quality report button
    quality_btn = ui_adapter.create_button(
        sidebar_frame,
        text="📊 Quality Report",
        command=lambda: app_instance.show_quality_report() if hasattr(app_instance, 'show_quality_report') else None
    )
    quality_btn.pack(pady=2, padx=20, fill="x")
    
    # Recommendations button
    rec_btn = ui_adapter.create_button(
        sidebar_frame,
        text="💡 Recommendations",
        command=lambda: app_instance.show_recommendations() if hasattr(app_instance, 'show_recommendations') else None
    )
    rec_btn.pack(pady=2, padx=20, fill="x")
    
    # Combined validation and cleaning button
    combined_btn = ui_adapter.create_button(
        sidebar_frame,
        text="🔍🧹 Validate & Clean",
        command=lambda: app_instance.validate_and_clean() if hasattr(app_instance, 'validate_and_clean') else None
    )
    combined_btn.pack(pady=2, padx=20, fill="x")


# Example usage in the main app:
"""
# In your main app initialization:

# 1. Add validation features to the app
validation_methods = add_validation_features_to_app(app)

# 2. Add validation buttons to sidebar (if you have one)
add_validation_buttons_to_sidebar(app, sidebar_frame)

# 3. Use validation methods programmatically
app.validate_current_data()  # Validate current data
app.clean_current_data()     # Clean current data
app.show_quality_report()    # Show quality report
app.show_recommendations()   # Show recommendations
app.validate_and_clean()     # Combined operation

# 4. Access validation service directly if needed
from frontend.core.container import get_service
from frontend.domain.services.data_validation_service import DataValidationService

validation_service = get_service(DataValidationService)
result = validation_service.validate_dataset(your_data)
"""
