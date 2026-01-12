"""
Claims Extraction Agent - Code-based MLflow model

This file defines the PyFunc model class for MLflow's "models from code" logging approach.
It contains all necessary schemas and the agent implementation.
"""

import json
import os
from typing import Any, Literal, Optional

import mlflow
from mlflow.pyfunc import PythonModel
from pydantic import BaseModel, Field

# --- Input/Output Schemas ---


class AgentInput(BaseModel):
    """Input: text content to classify and extract"""

    text: str = Field(description="Extracted text content from the document")


class Classification(BaseModel):
    """Classification result with confidence"""

    claim_type: Literal["APS", "EPS", "Other"] = Field(
        description="Type of form: APS (Attending Physician's Statement) or EPS (Employee's Statement)"
    )
    priority: Literal["Urgent", "Standard"] = Field(
        description="Processing priority (Urgent if: hospitalization, surgery, dismemberment, death)"
    )
    confidence: float = Field(
        ge=0.0, le=1.0, description="Classification confidence score"
    )


# --- Extraction Sub-Schemas ---


class InsuredInfo(BaseModel):
    """Information about the insured person"""

    full_name: Optional[str] = None
    employer_company: Optional[str] = None
    group_policy_no: Optional[str] = None
    ssn: Optional[str] = None
    date_of_birth: Optional[str] = None
    phone: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    sex: Optional[str] = None


class PatientInfo(BaseModel):
    """Information about the patient (if different from insured)"""

    full_name: Optional[str] = None
    relationship_to_insured: Optional[str] = None  # Self, Spouse, Child, etc.
    ssn: Optional[str] = None
    date_of_birth: Optional[str] = None
    sex: Optional[str] = None


class AccidentInfo(BaseModel):
    """Information about the accident"""

    date_of_accident: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None
    motor_vehicle_accident: Optional[bool] = None
    work_related: Optional[bool] = None
    hospitalized: Optional[bool] = None
    admission_date: Optional[str] = None
    discharge_date: Optional[str] = None
    hospital_name: Optional[str] = None


class TreatmentInfo(BaseModel):
    """Treatment details (APS-specific)"""

    date_of_service: Optional[str] = None
    diagnosis_description: Optional[str] = None
    diagnosis_code_icd: Optional[str] = None
    procedure_code_cpt: Optional[str] = None
    procedure_description: Optional[str] = None
    emergency_room: Optional[bool] = None
    emergency_room_date: Optional[str] = None
    urgent_care: Optional[bool] = None
    urgent_care_date: Optional[str] = None
    surgery_performed: Optional[bool] = None
    surgery_details: Optional[str] = None
    facility_name: Optional[str] = None
    other_conditions: Optional[str] = None


class PhysicianInfo(BaseModel):
    """Attending physician information (APS-specific)"""

    name: Optional[str] = None
    specialty: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    phone: Optional[str] = None
    fax: Optional[str] = None
    signature_date: Optional[str] = None


class DismembermentInfo(BaseModel):
    """Dismemberment/impairment details (APS-specific)"""

    hearing_loss: Optional[bool] = None
    hearing_loss_details: Optional[str] = None
    sight_loss: Optional[bool] = None
    sight_loss_details: Optional[str] = None
    limb_loss: Optional[bool] = None
    limb_loss_details: Optional[str] = None
    paralysis: Optional[bool] = None
    paralysis_details: Optional[str] = None


class BenefitsClaimed(BaseModel):
    """Additional benefits claimed (EPS-specific)"""

    lodging: Optional[bool] = None
    transportation: Optional[bool] = None
    youth_sport: Optional[bool] = None
    accidental_death: Optional[bool] = None
    death_date: Optional[str] = None


class Extraction(BaseModel):
    """Extracted data from accident benefits form"""

    insured: Optional[InsuredInfo] = None
    patient: Optional[PatientInfo] = None
    accident: Optional[AccidentInfo] = None
    treatment: Optional[TreatmentInfo] = None  # APS only
    physician: Optional[PhysicianInfo] = None  # APS only
    dismemberment: Optional[DismembermentInfo] = None  # APS only
    benefits_claimed: Optional[BenefitsClaimed] = None  # EPS only
    signature_date: Optional[str] = None


class AgentOutput(BaseModel):
    """Output from the extraction agent"""

    classification: Classification
    extraction: Extraction
    is_failed: bool = False
    error_message: Optional[str] = None


# --- Agent Implementation ---


class ClaimsExtractionAgent(PythonModel):
    """Non-conversational agent for classifying and extracting medical claims data."""

    def load_context(self, context):
        """Initialize clients and enable tracing."""
        from openai import OpenAI

        mlflow.tracing.enable()

        # Get Databricks host and token from environment (set by Model Serving)
        db_host = os.getenv("DATABRICKS_HOST")
        db_token = os.getenv("DATABRICKS_TOKEN")

        # Fallback to WorkspaceClient if env vars not set (for local testing)
        if not db_host or not db_token:
            from databricks.sdk import WorkspaceClient

            w = WorkspaceClient()
            db_host = w.config.host
            db_token = w.config.token

        self.llm_endpoint = os.getenv("LLM_ENDPOINT", "databricks-claude-sonnet-4-5")
        self.client = OpenAI(
            api_key=db_token,
            base_url=f"{db_host}/serving-endpoints",
        )

    @mlflow.trace(name="classify", span_type="LLM")
    def _classify(self, text: str) -> Classification:
        """Classify the claim type and priority."""
        system_prompt = """You are an insurance claims classification expert.
Analyze the document and classify it based on form type.

Classification rules:
- claim_type:
  - "APS" if the document is an Attending Physician's Statement (medical form completed by a physician)
  - "EPS" if the document is an Employee's Statement (accident benefits claim completed by employee)
  - "Other" if neither
- priority:
  - "Urgent" if: hospitalization, surgery, dismemberment, paralysis, sight/hearing loss, or death
  - "Standard" otherwise
- confidence: your confidence in the classification (0.0 to 1.0)"""

        response = self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": f"Classify this insurance document:\n\n{text[:8000]}",
                },
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "classification",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "claim_type": {
                                "type": "string",
                                "enum": ["APS", "EPS", "Other"],
                            },
                            "priority": {
                                "type": "string",
                                "enum": ["Urgent", "Standard"],
                            },
                            "confidence": {"type": "number"},
                        },
                        "required": ["claim_type", "priority", "confidence"],
                    },
                    "strict": True,
                },
            },
            max_tokens=200,
            temperature=0,
        )

        result = json.loads(response.choices[0].message.content)
        return Classification(**result)

    def _get_extraction_schema(self, claim_type: str) -> dict:
        """Build JSON schema for extraction based on claim type."""
        # Common sub-schemas
        insured_schema = {
            "type": "object",
            "properties": {
                "full_name": {"type": ["string", "null"]},
                "employer_company": {"type": ["string", "null"]},
                "group_policy_no": {"type": ["string", "null"]},
                "ssn": {"type": ["string", "null"]},
                "date_of_birth": {"type": ["string", "null"]},
                "phone": {"type": ["string", "null"]},
                "address": {"type": ["string", "null"]},
                "city": {"type": ["string", "null"]},
                "state": {"type": ["string", "null"]},
                "zip_code": {"type": ["string", "null"]},
                "sex": {"type": ["string", "null"]},
            },
        }
        patient_schema = {
            "type": "object",
            "properties": {
                "full_name": {"type": ["string", "null"]},
                "relationship_to_insured": {"type": ["string", "null"]},
                "ssn": {"type": ["string", "null"]},
                "date_of_birth": {"type": ["string", "null"]},
                "sex": {"type": ["string", "null"]},
            },
        }
        accident_schema = {
            "type": "object",
            "properties": {
                "date_of_accident": {"type": ["string", "null"]},
                "location": {"type": ["string", "null"]},
                "description": {"type": ["string", "null"]},
                "motor_vehicle_accident": {"type": ["boolean", "null"]},
                "work_related": {"type": ["boolean", "null"]},
                "hospitalized": {"type": ["boolean", "null"]},
                "admission_date": {"type": ["string", "null"]},
                "discharge_date": {"type": ["string", "null"]},
                "hospital_name": {"type": ["string", "null"]},
            },
        }

        base_schema = {
            "type": "object",
            "properties": {
                "insured": insured_schema,
                "patient": patient_schema,
                "accident": accident_schema,
                "signature_date": {"type": ["string", "null"]},
            },
        }

        if claim_type == "APS":
            # Add APS-specific fields
            base_schema["properties"]["treatment"] = {
                "type": "object",
                "properties": {
                    "date_of_service": {"type": ["string", "null"]},
                    "diagnosis_description": {"type": ["string", "null"]},
                    "diagnosis_code_icd": {"type": ["string", "null"]},
                    "procedure_code_cpt": {"type": ["string", "null"]},
                    "procedure_description": {"type": ["string", "null"]},
                    "emergency_room": {"type": ["boolean", "null"]},
                    "emergency_room_date": {"type": ["string", "null"]},
                    "urgent_care": {"type": ["boolean", "null"]},
                    "urgent_care_date": {"type": ["string", "null"]},
                    "surgery_performed": {"type": ["boolean", "null"]},
                    "surgery_details": {"type": ["string", "null"]},
                    "facility_name": {"type": ["string", "null"]},
                    "other_conditions": {"type": ["string", "null"]},
                },
            }
            base_schema["properties"]["physician"] = {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "specialty": {"type": ["string", "null"]},
                    "address": {"type": ["string", "null"]},
                    "city": {"type": ["string", "null"]},
                    "state": {"type": ["string", "null"]},
                    "zip_code": {"type": ["string", "null"]},
                    "phone": {"type": ["string", "null"]},
                    "fax": {"type": ["string", "null"]},
                    "signature_date": {"type": ["string", "null"]},
                },
            }
            base_schema["properties"]["dismemberment"] = {
                "type": "object",
                "properties": {
                    "hearing_loss": {"type": ["boolean", "null"]},
                    "hearing_loss_details": {"type": ["string", "null"]},
                    "sight_loss": {"type": ["boolean", "null"]},
                    "sight_loss_details": {"type": ["string", "null"]},
                    "limb_loss": {"type": ["boolean", "null"]},
                    "limb_loss_details": {"type": ["string", "null"]},
                    "paralysis": {"type": ["boolean", "null"]},
                    "paralysis_details": {"type": ["string", "null"]},
                },
            }
        elif claim_type == "EPS":
            # Add EPS-specific fields
            base_schema["properties"]["benefits_claimed"] = {
                "type": "object",
                "properties": {
                    "lodging": {"type": ["boolean", "null"]},
                    "transportation": {"type": ["boolean", "null"]},
                    "youth_sport": {"type": ["boolean", "null"]},
                    "accidental_death": {"type": ["boolean", "null"]},
                    "death_date": {"type": ["string", "null"]},
                },
            }

        return base_schema

    @mlflow.trace(name="extract", span_type="LLM")
    def _extract(self, text: str, classification: Classification) -> Extraction:
        """Extract fields from the document based on classification."""
        claim_type = classification.claim_type

        if claim_type == "APS":
            system_prompt = """You are an insurance document extraction expert.
Extract data from this Attending Physician's Statement (APS) form.

Extract the following sections:
- insured: Insured person's information (name, employer, policy number, SSN, DOB, contact info)
- patient: Patient information if different from insured (name, relationship, SSN, DOB)
- accident: Accident details (date, location, description, whether motor vehicle/work related, hospitalization)
- treatment: Treatment details (date of service, diagnosis, procedure codes, ER/urgent care, surgery, facility)
- physician: Attending physician info (name, specialty, address, phone)
- dismemberment: If applicable (hearing/sight/limb loss, paralysis)
- signature_date: Date the form was signed

Use null for any fields not found in the document."""
        elif claim_type == "EPS":
            system_prompt = """You are an insurance document extraction expert.
Extract data from this Employee's Statement (EPS) form.

Extract the following sections:
- insured: Insured person's information (name, employer, policy number, SSN, DOB, contact info)
- patient: Patient information if different from insured (name, relationship, SSN, DOB)
- accident: Accident details (date, location, description, whether motor vehicle/work related, hospitalization)
- benefits_claimed: Additional benefits being claimed (lodging, transportation, youth sport, accidental death)
- signature_date: Date the form was signed

Use null for any fields not found in the document."""
        else:
            system_prompt = """You are an insurance document extraction expert.
Extract any relevant data from this document.

Try to extract:
- insured: Person's information (name, contact details)
- accident: Any accident details mentioned
- signature_date: Date the document was signed

Use null for any fields not found in the document."""

        extraction_schema = self._get_extraction_schema(claim_type)

        response = self.client.chat.completions.create(
            model=self.llm_endpoint,
            messages=[
                {"role": "system", "content": system_prompt},
                {
                    "role": "user",
                    "content": f"Extract data from this {claim_type} form:\n\n{text[:12000]}",
                },
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "extraction",
                    "schema": extraction_schema,
                    "strict": True,
                },
            },
            max_tokens=2000,
            temperature=0,
        )

        result = json.loads(response.choices[0].message.content)

        # Convert nested dicts to Pydantic models
        extraction_data = {}
        if result.get("insured"):
            extraction_data["insured"] = InsuredInfo(**result["insured"])
        if result.get("patient"):
            extraction_data["patient"] = PatientInfo(**result["patient"])
        if result.get("accident"):
            extraction_data["accident"] = AccidentInfo(**result["accident"])
        if result.get("treatment"):
            extraction_data["treatment"] = TreatmentInfo(**result["treatment"])
        if result.get("physician"):
            extraction_data["physician"] = PhysicianInfo(**result["physician"])
        if result.get("dismemberment"):
            extraction_data["dismemberment"] = DismembermentInfo(
                **result["dismemberment"]
            )
        if result.get("benefits_claimed"):
            extraction_data["benefits_claimed"] = BenefitsClaimed(
                **result["benefits_claimed"]
            )
        extraction_data["signature_date"] = result.get("signature_date")

        return Extraction(**extraction_data)

    @mlflow.trace(name="predict")
    def predict(self, context, model_input):
        """Process documents for classification and extraction.

        Input: DataFrame or list of dicts with 'text' field containing document text.
               (No type hints to ensure compatibility with ai_query's dataframe_records format)
        Output: list of dicts with classification and extraction results.
        """
        # Handle both DataFrame (from dataframe_records) and list of dicts (from inputs)
        if hasattr(model_input, "to_dict"):
            records = model_input.to_dict(orient="records")
        else:
            records = model_input

        outputs = []

        for doc in records:
            try:
                # Extract text from input
                text = doc.get("text", "") if isinstance(doc, dict) else str(doc)

                if not text:
                    raise ValueError("No text provided in input")

                # Two-call pattern: classify first, then extract
                classification = self._classify(text)
                extraction = self._extract(text, classification)

                output = AgentOutput(
                    classification=classification,
                    extraction=extraction,
                )
            except Exception as e:
                output = AgentOutput(
                    classification=Classification(
                        claim_type="Other", priority="Standard", confidence=0.0
                    ),
                    extraction=Extraction(),
                    is_failed=True,
                    error_message=str(e),
                )

            outputs.append(output.model_dump())

        return outputs


# Tell MLflow which class to use as the model
# This must be called unconditionally for code-based logging to work
mlflow.models.set_model(ClaimsExtractionAgent())
