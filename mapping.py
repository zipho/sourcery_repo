from dvf.logging import Logger
from dvf.utils.env import getvar
from dvf.utils.state import IngressState
from dvf.exceptions import DvfException
from collections import defaultdict
from typing import Any, Optional
from datetime import datetime, timezone

logger = Logger(__file__)

plant_source_system = {
    "A110TUP": "StardMuc",
    "A120TDP": "StardDgf",
    "A110TUQ": "StardMuc",
    "A120TDQ": "StardDgf",
    "A130": "StardBer",
    "A150": "StardLpz",
    "LY78": "StardSlp",
    "MU80": "StardOxf",
    "A160": "StardReg",
}

source_system_to_bmw_plant_code = {
    "StardMuc": "01.10",
    "StardDgf": "02.40",
    "StardBer": "03.10",
    "StardLpz": "07.10",
    "StardSlp": "30.10",
    "StardOxf": "34.00",
    "StardReg": "06.10",
}


def process(_, stard_kafka_vehicles: "dict[str, Any]" or None, state: IngressState):
    if stard_kafka_vehicles is None:
        raise DvfException("Empty payload.", state.tags)

    headers = build_headers(state)
    vehicle_attributes = build_vehicle_attributes(stard_kafka_vehicles)
    materials_grouped_by_material_number = build_grouped_materials(stard_kafka_vehicles)
    material_change_indexes = build_material_change_indexes(materials_grouped_by_material_number)

    body = {
        "type": "vehicle",
        "attributes": vehicle_attributes,
        "children": material_change_indexes,
    }
    return headers, [body]


def build_material_change_indexes(materials_grouped_by_material_number: "dict[str, Any]")  ->  "list[dict]":
    material_change_indexes = []
    for material_number, grouped_materials in materials_grouped_by_material_number.items():
        vehicle_production_steps = []
        production_step_exists = False
        for grouped_material in grouped_materials:
            production_step = grouped_material["plannedOrderId"][0:2]
            production_step_exists = prod_step_exist_status(grouped_material, production_step, vehicle_production_steps, production_step_exists)
            if not production_step_exists:
                vehicle_production_step = build_vehicle_production_step(grouped_material, production_step)
                vehicle_production_steps.append(vehicle_production_step)

        material_change_index = get_material_change_index(material_change_indexes, material_number)
        if material_change_index:
            material_change_index["relationshipAttributes"]["quantity"] = material_change_index[
                                                                              "relationshipAttributes"
                                                                          ]["quantity"] + sum(
                grouped_material["quantity"] for grouped_material in grouped_materials
            )
            material_change_index["relationshipChildren"] += vehicle_production_steps
        else:
            material_change_index = build_material_change_index(grouped_materials, material_number,
                                                                vehicle_production_steps)
            material_change_indexes.append(material_change_index)
    return material_change_indexes


def build_material_change_index(grouped_materials: "list[dict]", material_number: "str", vehicle_production_steps: "list[dict]") -> "dict[str|Any]":
    return {
        "type": "materialChangeIndex",
        "attributes": {
            "partNumber": material_number[0:7],
            "changeIndex": int(material_number[7:9]),
        },
        "relationshipAttributes": {
            "quantity": sum(
                grouped_material["quantity"] for grouped_material in grouped_materials
            ),
        },
        "relationshipChildren": vehicle_production_steps,
    }


def build_vehicle_production_step(grouped_material: "dict[str|Any]", production_step: "str") -> "dict[str|Any]":
    return {
        "type": "vehicleProductionStep",
        "attributes": {"productionStep": production_step},
        "relationshipAttributes": {
            "quantity": grouped_material["quantity"],
            "plannedOrderNumber": grouped_material["plannedOrderId"],
            "reservationItemNumber": grouped_material["reservationPosition"],
            "reservationNumber": grouped_material["reservationNumber"],
            "materialGroupTopLevel": grouped_material["peggedRequirement"],
            "itemCategory": grouped_material["itemCategory"],
        },
    }


def get_material_change_index(material_change_indexes: "list", material_number: "str") -> "list|None":
    material_change_index = next(
        filter(
            lambda vehicle_production_step: material_number[0:7]
                                            == vehicle_production_step["attributes"]["partNumber"]
                                            and int(material_number[7:9])
                                            == vehicle_production_step["attributes"]["changeIndex"],
            material_change_indexes,
        ),
        None,
    )
    return material_change_index


def prod_step_exist_status(grouped_material: "dict[str|Any]", production_step: "str", vehicle_production_steps: "list", production_step_exists: "bool" = False) -> "bool":
    for vehicle_production_step in vehicle_production_steps:
        if vehicle_production_step["attributes"]["productionStep"] == production_step:
            vehicle_production_step["relationshipAttributes"][
                "quantity"
            ] += grouped_material["quantity"]
            production_step_exists = True
            break
        else:
            production_step_exists = False
    return production_step_exists


def build_grouped_materials(stard_kafka_vehicles: "dict[str, Any]") -> "defaultdict":
    all_materials = stard_kafka_vehicles["materials"]
    filtered_materials = [m for m in all_materials if m["plannedOrderId"].startswith("VH_")]
    materials_grouped_by_material_number = defaultdict(list)
    for material in filtered_materials:
        materials_grouped_by_material_number[material["materialNumber"]].append(material)
    return materials_grouped_by_material_number


def build_vehicle_attributes(stard_kafka_vehicles: "dict[str, Any]") -> "dict[str, Any]":
    vin = stard_kafka_vehicles["vin17"]
    build_datetime = stard_kafka_vehicles["utcCheckpointTimestamp"]
    sap_plant_code = stard_kafka_vehicles["sapPlantId"]
    bmw_plant_code = get_bmw_plant_code(sap_plant_code)
    vehicle_attributes = {
        "Vin": vin,
        "SapPlantCode": sap_plant_code,
        "plantCode": bmw_plant_code,
        "buildDate": build_datetime[0:10].replace("-", ""),
        "buildTime": build_datetime[11:19].replace(":", ""),
        "buildTimestamp": stard_kafka_vehicles["utcBuildTimestamp"].replace("Z", ".000Z"),
        "modelCode": stard_kafka_vehicles["modelCode"],
        "buildStatus": int(stard_kafka_vehicles["orderStatus"]),
        "orderNumber": stard_kafka_vehicles["orderNumber"],
        "checkpointTimestamp": build_datetime.replace("Z", ".000Z"),
        "checkpointTimeZone": derive_timezone(
            stard_kafka_vehicles["utcCheckpointTimestamp"],
            stard_kafka_vehicles["localCheckpointTimestamp"],
        ),
    }
    return vehicle_attributes


def build_headers(state: IngressState) -> "dict":
    s3_object_key = get_s3_object_key(state)
    if not get_s3_object_key(state):
        raise DvfException("S3 object key could not be resolved.", state.tags)
    return {"s3_object_key": s3_object_key, "s3_object_format": "json"}


def get_s3_object_key(state: IngressState) -> "str":
    return state.tags["s3_object"]


def derive_timezone(utc_time: str, local_time: str) -> "str":
    utc_datetime = datetime.strptime(utc_time, "%Y-%m-%dT%H:%M:%SZ")
    local_datetime = datetime.strptime(local_time, "%Y-%m-%dT%H:%M:%S")
    return str(timezone(local_datetime - utc_datetime)).replace("UTC", "").replace(":", "")


def get_bmw_plant_code(sap_plant_code: str) -> Optional[str]:
    if sap_plant_code in plant_source_system:
        source_system = plant_source_system[sap_plant_code]
        return source_system_to_bmw_plant_code[source_system]
    else:
        return None
