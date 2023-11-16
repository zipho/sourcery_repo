from __future__ import annotations
from dvf.logging import Logger
from dvf.utils.env import getvar
from dvf.utils.state import IngressState
from dvf.exceptions import DvfException
from collections import defaultdict
from typing import Any

logger = Logger(__file__)


def process(_, proton_vehicle_payload: dict[str, Any], state: IngressState):
    if proton_vehicle_payload is None:
        raise DvfException("Empty payload.", state.tags)

    # START
    vehicle_identity_number = get_vin(proton_vehicle_payload)
    build_datetime = proton_vehicle_payload["actualCustomerOrder"]["pimaData"]["OrderProduction"][
        "BuildTime"
    ]
    headers = build_headers(state)

    planned_orders = proton_vehicle_payload["actualCustomerOrderMaterial"]["plannedOrders"]
    planned_orders_startwith_vh_ = list(
        filter(
            lambda planned_order: planned_order["plannedOrderId"].startswith("VH_"),
            planned_orders,
        )
    )

    # get vehicle attributes
    vehicle_attributes = get_vehicles_attributes(
        build_datetime,
        planned_orders_startwith_vh_,
        proton_vehicle_payload,
        vehicle_identity_number,
    )

    # get header materials
    header_materials = get_header_material_per_planned_order_id(proton_vehicle_payload)

    material_change_indexes = []
    for planned_order in planned_orders:
        production_step = planned_order["plannedOrderId"][:2]
        unique_demands = defaultdict(list)
        for demand in planned_order["demands"]:
            unique_demands[(demand["materialNumber"])].append(demand)

        generate_children(
            header_materials,
            material_change_indexes,
            planned_order,
            production_step,
            unique_demands,
        )
    body = {
        "type": "vehicle",
        "attributes": vehicle_attributes,
        "children": material_change_indexes,
    }

    # END
    return headers, [body]


def generate_children(
    header_materials: dict[str:Any],
    material_change_indexes: list,
    planned_order: dict[str:Any],
    production_step: str,
    unique_demands: defaultdict,
):
    for material_number, demands in unique_demands.items():
        vehicle_production_steps = []
        is_step_added = False
        for demand in demands:
            is_step_added = get_step_added_status(
                demand, is_step_added, production_step, vehicle_production_steps
            )
            if not is_step_added:
                build_vehicle_production_step(
                    demand,
                    header_materials,
                    planned_order,
                    production_step,
                    vehicle_production_steps,
                )

        material_change_index = get_mci(material_change_indexes, material_number)
        handle_material_change_indexes(
            demands,
            material_change_index,
            material_change_indexes,
            material_number,
            vehicle_production_steps,
        )


def handle_material_change_indexes(
    demands,
    material_change_index,
    material_change_indexes,
    material_number,
    vehicle_production_steps,
):
    if material_change_index:
        material_change_index["relationshipAttributes"]["quantity"] = material_change_index[
            "relationshipAttributes"
        ]["quantity"] + sum(demand["quantity"] for demand in demands)
        material_change_index["relationshipChildren"] += vehicle_production_steps
    else:
        material_change_index = {
            "type": "materialChangeIndex",
            "attributes": {
                "partNumber": material_number[:7],
                "changeIndex": int(material_number[7:9]),
            },
            "relationshipAttributes": {
                "quantity": sum(demand["quantity"] for demand in demands),
            },
            "relationshipChildren": vehicle_production_steps,
        }
        material_change_indexes.append(material_change_index)


def build_vehicle_production_step(
    demand, header_materials, planned_order, production_step, vehicle_production_steps
):
    vehicle_production_step = {
        "type": "vehicleProductionStep",
        "attributes": {"productionStep": production_step},
        "relationshipAttributes": {
            "quantity": demand["quantity"],
            "plannedOrderNumber": planned_order["plannedOrderId"],
            "materialGroupTopLevel": header_materials.get(planned_order["plannedOrderId"]),
        },
    }
    vehicle_production_steps.append(vehicle_production_step)


def get_mci(material_change_indexes, material_number):
    return next(
        filter(
            lambda item: material_number[:7]
            == item["attributes"]["partNumber"]
            and int(material_number[7:9]) == item["attributes"]["changeIndex"],
            material_change_indexes,
        ),
        None,
    )


def get_step_added_status(demand, is_step_added, production_step, vehicle_production_steps):
    for item in vehicle_production_steps:
        if item["attributes"]["productionStep"] == production_step:
            item["relationshipAttributes"]["quantity"] += demand["quantity"]
            is_step_added = True
        else:
            is_step_added = False
    return is_step_added


def get_vehicles_attributes(
    build_datetime: dict[str:Any],
    planned_orders_startwith_vh: dict[str:Any],
    proton_vehicle_payload: dict[str:Any],
    vehicle_identity_number: dict[str:Any],
):
    vehicle_attributes = {
        "Vin": vehicle_identity_number,
        "buildDate": build_datetime[:10].replace("-", ""),
        "buildTime": build_datetime[11:19].replace(":", ""),
        "modelCode": proton_vehicle_payload["actualCustomerOrder"]["pimaData"][
            "PipelineOrderAttributes"
        ]["ManufacturerModelCode"],
        "buildStatus": int(
            proton_vehicle_payload["actualCustomerOrder"]["orderAttributes"][
                "orderStatus"
            ]
        ),
        "orderNumber": proton_vehicle_payload["actualCustomerOrder"][
            "orderNumber"
        ],
        "plantCode": planned_orders_startwith_vh[0]["plantId"],
        "SapPlantCode": planned_orders_startwith_vh[0]["sapPlantId"],
    }
    # override attribute values
    if vehicle_attributes["buildStatus"] == 5500:
        vehicle_attributes["buildStatus"] = 6000
    return vehicle_attributes


def build_headers(state: IngressState):
    return {
        "BMW-DVF-Contract-ID": "PRIW",
        "BMW-DVF-Source-System": "protonOxf",
        "BMW-DVF-Source-Tracking-String": state.tags,
    }


def get_vin(proton_vehicle_payload: dict[str, Any]):
    return (
        proton_vehicle_payload["actualCustomerOrder"]["pimaData"][
            "ActualOrder"
        ]["VIN10"]
        + proton_vehicle_payload["actualCustomerOrder"]["pimaData"][
            "ActualOrder"
        ]["VIN7"]
    )


def get_header_material_per_planned_order_id(proton_vehicle_payload: dict[str:Any]):
    header_materials = {}
    orders_per_plant = proton_vehicle_payload["actualCustomerOrder"]["orderPerPlant"]
    for order_per_plant in orders_per_plant:
        planned_orders = order_per_plant["plannedOrder"]
        for planned_order in planned_orders:
            header_materials[planned_order["plannedOrderId"]] = planned_order["headerMaterial"]
    return header_materials
