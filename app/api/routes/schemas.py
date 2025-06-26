"""
Schema management endpoints.
"""
from fastapi import APIRouter, HTTPException, Path, Body, status
from typing import Dict, Any, List
from app.application.services.schema_service import schema_service
from app.domain.exceptions.exceptions import SchemaNotFoundException
from app.config.logging_utils import log_application_event
from app.api.responses.response import FastJSONResponse

router = APIRouter(prefix="/schemas", tags=["schemas"])

@router.post(
    "/{schema_name}",
    status_code=status.HTTP_201_CREATED,
    response_model=Dict,
)
async def register_schema_version(
    schema_definition: Dict[str, Any] = Body(..., example={"description": "An example schema"}),
    schema_name: str = Path(..., description="Schema name to register a new version for"),
):
    """
    Registers a new version of a schema. The system automatically assigns the next version number.
    """
    try:
        log_application_event(f"Registering new schema version for: {schema_name}")
        new_schema = schema_service.register_new_schema_version(schema_name, schema_definition)
        return FastJSONResponse(new_schema.to_dict())
    except Exception as e:
        log_application_event(f"Error registering schema version: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[str])
async def list_schema_families():
    """
    Lists all available schema families.
    """
    try:
        log_application_event("Listing all schema families")
        return FastJSONResponse(schema_service.list_schema_families())
    except Exception as e:
        log_application_event(f"Error listing schema families: {e}", "error")
        raise HTTPException(status_code=500, detail="Error listing schema families.")


@router.get("/{schema_name}", response_model=List[int])
async def list_versions_for_schema(
    schema_name: str = Path(..., description="Schema name to list versions for"),
):
    """
    Lists all available versions for a specific schema family.
    """
    try:
        log_application_event(f"Listing versions for schema: {schema_name}")
        versions = schema_service.list_schema_versions(schema_name)
        return FastJSONResponse(versions)
    except SchemaNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_application_event(f"Error listing schema versions: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{schema_name}/latest", response_model=Dict)
async def get_latest_schema(
    schema_name: str = Path(..., description="Schema name to get the latest version of"),
):
    """
    Gets the full schema definition for the most recent version of a schema family.
    """
    try:
        log_application_event(f"Getting latest schema for: {schema_name}")
        schema = schema_service.get_schema(schema_name)  # No version means latest
        return FastJSONResponse(schema.to_dict())
    except SchemaNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_application_event(f"Error getting latest schema: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{schema_name}/{version}", response_model=Dict)
async def get_specific_schema_version(
    schema_name: str = Path(..., description="Schema family name"),
    version: int = Path(..., description="The specific version of the schema to retrieve"),
):
    """
    Gets the full schema definition for a specific version of a schema family.
    """
    try:
        log_application_event(f"Getting schema: {schema_name} version: {version}")
        schema = schema_service.get_schema(schema_name, version)
        return FastJSONResponse(schema.to_dict())
    except SchemaNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_application_event(f"Error getting specific schema version: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/{schema_name}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def delete_schema_family(
    schema_name: str = Path(..., description="Schema family to delete (archive)"),
):
    """
    (Soft) Deletes an entire schema family by archiving all its versions.
    """
    try:
        log_application_event(f"Deleting schema family: {schema_name}")
        schema_service.delete_schema_family(schema_name)
        return FastJSONResponse(content=None, status_code=status.HTTP_204_NO_CONTENT)
    except SchemaNotFoundException as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        log_application_event(f"Error deleting schema family: {e}", "error")
        raise HTTPException(status_code=500, detail=str(e))
