r"""Sample to manage correlation rules as part of a detection as code pipeline.

▄█▄    ████▄ █▄▄▄▄ █▄▄▄▄ ▄███▄   █    ██     ▄▄▄▄▀ ▄█ ████▄    ▄
█▀ ▀▄  █   █ █  ▄▀ █  ▄▀ █▀   ▀  █    █ █ ▀▀▀ █    ██ █   █     █
█   ▀  █   █ █▀▀▌  █▀▀▌  ██▄▄    █    █▄▄█    █    ██ █   █ ██   █
█▄  ▄▀ ▀████ █  █  █  █  █▄   ▄▀ ███▄ █  █   █     ▐█ ▀████ █ █  █
▀███▀          █     █   ▀███▀       ▀   █  ▀       ▐       █  █ █
              ▀     ▀                   █                   █   ██
                                       ▀
                            █▄▄▄▄  ▄   █     ▄███▄     ▄▄▄▄▄
                           █  ▄▀   █  █     █▀   ▀   █     ▀▄
                          █▀▀▌ █   █ █     ██▄▄   ▄  ▀▀▀▀▄
                          █  █ █   █ ███▄  █▄   ▄▀ ▀▄▄▄▄▀
                            █  █▄ ▄█     ▀ ▀███▀
                           ▀    ▀▀▀

██████╗ ███████╗████████╗███████╗ ██████╗████████╗██╗ ██████╗ ███╗   ██╗
██╔══██╗██╔════╝╚══██╔══╝██╔════╝██╔════╝╚══██╔══╝██║██╔═══██╗████╗  ██║
██║  ██║█████╗     ██║   █████╗  ██║        ██║   ██║██║   ██║██╔██╗ ██║
██║  ██║██╔══╝     ██║   ██╔══╝  ██║        ██║   ██║██║   ██║██║╚██╗██║
██████╔╝███████╗   ██║   ███████╗╚██████╗   ██║   ██║╚██████╔╝██║ ╚████║
╚═════╝ ╚══════╝   ╚═╝   ╚══════╝ ╚═════╝   ╚═╝   ╚═╝ ╚═════╝ ╚═╝  ╚═══╝

 █████╗ ███████╗     ██████╗ ██████╗ ██████╗ ███████╗
██╔══██╗██╔════╝    ██╔════╝██╔═══██╗██╔══██╗██╔════╝
███████║███████╗    ██║     ██║   ██║██║  ██║█████╗
██╔══██║╚════██║    ██║     ██║   ██║██║  ██║██╔══╝     Built with
██║  ██║███████║    ╚██████╗╚██████╔╝██████╔╝███████╗       FalconPy v1.4.8
╚═╝  ╚═╝╚══════╝     ╚═════╝ ╚═════╝ ╚═════╝ ╚══════╝

This solution leverages the CorrelationRules service collection to manage
CrowdStrike Correlation Rules as code, enabling version control and automated
deployment of detection rules.

REQUIRES
crowdstrike-falconpy v1.4.8 or greater    https://github.com/CrowdStrike/falconpy

Creation date: 02.28.2025 - Initial version, crowdstrikedcs@crowdstrike
"""
# pylint: disable=W0718,W0719
import json
import logging
import os
import traceback
from typing import List, Dict, Optional, Tuple
from falconpy import CorrelationRules


class CorrelationRulesClient:
    """Class to watch for changes to a local JSON of correlation rules."""

    def __init__(self):
        """Construct an instance of the class."""
        self.setup_logger()
        self.logger = logging.getLogger(__name__)
        self.falcon = self.initialize_falcon_client()
        self.rules_dir = "rules"

    def setup_logger(self):
        """Set up logging configuration using LOG_LEVEL environment variable.

        Default to INFO if not specified.
        """
        # Get log level from environment variable, default to INFO
        log_level = os.environ.get('LOG_LEVEL', 'INFO').upper()

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

        self.logger = logging.getLogger(__name__)
        self.logger.debug("Logging initialized at %s level", log_level)

    def initialize_falcon_client(self):
        """Set up FalconPy Harness."""
        try:
            base_url = os.environ.get("FALCON_BASE_URL", "auto")

            if not os.environ.get('FALCON_CLIENT_ID') or not os.environ.get('FALCON_CLIENT_SECRET'):
                raise ValueError("FALCON_CLIENT_ID and FALCON_CLIENT_SECRET must be set")

            # Authenticates using Environment Authentication
            # https://falconpy.io/Usage/Authenticating-to-the-API.html#environment-authentication
            falcon = CorrelationRules(base_url=base_url, debug=True)
            self.logger.info("FalconPy client initialized successfully")
            return falcon

        except Exception as e:
            self.logger.error("Failed to initialize FalconPy client: %s", str(e))
            raise

    def get_all_rules(self):
        """Load rules from API."""
        rules = []
        offset = 0
        limit = 100

        self.logger.info("Starting to fetch correlation rules")

        try:
            while True:
                sort = "last_updated_on|asc"

                self.logger.debug("Fetching rules with offset %s", offset)

                response = self.falcon.get_rules_combined(limit=limit, offset=offset, sort=sort)

                if response["status_code"] != 200:
                    self.logger.error("API request failed with status %s", response['status_code'])
                    raise Exception("API request failed")

                current_rules = response["body"]["resources"]
                rules.extend(current_rules)

                self.logger.debug("Fetched %s rules in this batch", len(current_rules))
                self.logger.debug("Total rules fetched so far: %s", len(rules))

                if len(current_rules) < limit:
                    break

                offset += limit

        except Exception as e:
            self.logger.error("Error while fetching rules: %s", str(e))
            raise

        self.logger.info("Successfully fetched total of %s rules", len(rules))
        return rules

    def sanitize_rule_name(self, name: str) -> str:
        """Convert a rule name into a safe filename (no spaces or special chars).

        Example: 'My Rule - Test #2' -> 'My_Rule_-_Test__2'
        """
        safe = ""
        for ch in name:
            safe += ch if ch.isalnum() or ch in ("-", "_", ".") else "_"
        return safe

    def rule_file_path(self, rule: Dict) -> str:
        """Return the expected file path for a given rule, keyed by its name."""
        name = rule.get("name", rule.get("id", "unnamed"))
        return os.path.join(self.rules_dir, f"{self.sanitize_rule_name(name)}.json")

    def write_rule_file(self, rule: Dict):
        """Write a single rule to its own JSON file.

        Used after a successful create or update so the file reflects the
        authoritative API state (including any fields the API assigns, e.g. id).
        """
        try:
            os.makedirs(self.rules_dir, exist_ok=True)
            file_path = self.rule_file_path(rule)
            with open(file_path, 'w', encoding="utf-8") as f:
                json.dump(rule, f, indent=2)
            self.logger.info("Wrote rule file: %s", os.path.basename(file_path))
        except Exception as e:
            self.logger.error("Error writing rule file for '%s': %s", rule.get('name'), str(e))
            raise

    def delete_rule_file(self, rule: Dict):
        """Delete the JSON file that corresponds to the given rule dict.

        Used immediately after a successful API deletion so the local directory
        stays in sync without waiting for the end-of-run batch reconciliation.
        """
        try:
            file_path = self.rule_file_path(rule)
            if os.path.exists(file_path):
                os.remove(file_path)
                self.logger.info("Deleted rule file: %s", os.path.basename(file_path))
            else:
                self.logger.warning(
                    "Rule file not found for deletion: %s", os.path.basename(file_path)
                )
        except Exception as e:
            self.logger.error("Error deleting rule file for '%s': %s", rule.get('name'), str(e))
            raise

    def load_local_rules(self) -> List[Dict]:
        """Load rules from individual per-rule JSON files in the rules directory.

        Returns empty list if directory doesn't exist or contains no rule files.
        """
        try:
            if not os.path.exists(self.rules_dir):
                self.logger.info("%s does not exist, creating directory", self.rules_dir)
                os.makedirs(self.rules_dir, exist_ok=True)
                return []

            rule_files = sorted(
                f for f in os.listdir(self.rules_dir)
                if f.endswith(".json")
            )

            if not rule_files:
                self.logger.info("No rule files found in %s", self.rules_dir)
                return []

            local_rules = []
            for filename in rule_files:
                file_path = os.path.join(self.rules_dir, filename)
                try:
                    with open(file_path, 'r', encoding="utf-8") as f:
                        rule = json.load(f)
                    if rule:
                        local_rules.append(rule)
                        self.logger.debug("Loaded rule from %s", filename)
                    else:
                        self.logger.warning("Empty rule file: %s", filename)
                except json.JSONDecodeError as e:
                    self.logger.error("Error decoding %s: %s", filename, str(e))
                except Exception as e:
                    self.logger.error("Error loading %s: %s", filename, str(e))

            self.logger.info("Loaded %s rules from %s", len(local_rules), self.rules_dir)
            return local_rules

        except Exception as e:
            self.logger.error("Error loading rules from %s: %s", self.rules_dir, str(e))
            return []

    def is_rule_different(self, local_rule: Dict, api_rule: Dict) -> bool:
        """Compare relevant fields between local and API rules.

        Returns True if rules are different.
        """
        compare_fields = [
            'name',
            'description',
            'severity',
            'tactic',
            'technique'
            'search.outcome',
            'search.filter',
            'search.lookback'
        ]

        for field in compare_fields:
            local_value = self.get_nested_value(local_rule, field)
            api_value = self.get_nested_value(api_rule, field)
            if local_value != api_value:
                return True
        return False

    def get_nested_value(self, rule: Dict, field_path: str) -> any:
        """Safely get nested field values using dot notation."""
        try:
            current = rule
            for part in field_path.split('.'):
                current = current.get(part, {})
            return None if current == {} else current
        except Exception as e:
            self.logger.debug("Error accessing nested field %s: %s", field_path, str(e))
            return None

    def delete_rule_from_api(self, rule_id: str) -> bool:
        """Delete a rule from the API.

        Returns True if successful.
        """
        try:
            response = self.falcon.delete_rules(ids=rule_id)

            if response["status_code"] == 200:
                self.logger.info("Successfully deleted rule %s", rule_id)
                return True

            self.logger.error("Failed to delete rule %s: %s", rule_id, response)
            return False

        except Exception as e:
            self.logger.error("Error deleting rule %s: %s", rule_id, str(e))
            return False

    def compare_rules(self,
                      api_rules: List[Dict],
                      local_rules: List[Dict]
                      ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """Compare API rules with local rules to identify updates, deletions, and creations.

        Returns tuple of (rules_to_update, current_rules, rules_to_delete, rules_to_create).
        rules_to_delete contains full rule dicts (not just IDs) so callers can remove
        the corresponding local file immediately after a successful API deletion.
        """
        self.logger.info("Starting rules comparison")
        # Create dictionaries keyed by rule ID for easier lookup
        api_rules_dict = {rule['id']: rule for rule in api_rules}

        # Separate local rules into those with and without IDs
        local_rules_with_id = {}
        rules_to_create = []

        for rule in local_rules:
            if rule.get('id'):  # Existing rule
                if not rule.get('deleted', False):  # Not marked for deletion
                    local_rules_with_id[rule['id']] = rule
            else:  # New rule without ID
                rules_to_create.append(rule)
                self.logger.info("New rule to create: %s", rule.get('name', 'unnamed'))

        rules_to_update = []
        rules_to_delete = []

        # Identify rules marked for deletion — keep full dict so we can remove the file
        for rule in local_rules:
            if rule.get('deleted', False) and rule.get('id') in api_rules_dict:
                rules_to_delete.append(rule)
                self.logger.info("Rule %s marked for deletion", rule['id'])

        # Compare each local rule with API
        for rule_id, local_rule in local_rules_with_id.items():
            if rule_id in api_rules_dict:
                if self.is_rule_different(local_rule, api_rules_dict[rule_id]):
                    self.logger.info("Rule %s has changes and will be updated", rule_id)
                    rules_to_update.append(local_rule)
            else:
                self.logger.info("Rule %s exists in local but not in API", rule_id)

        self.logger.info("Comparison summary:")
        self.logger.info("Total API rules: %s", len(api_rules))
        self.logger.info("Total local rules: %s", len(local_rules))
        self.logger.info("Rules to update: %s", len(rules_to_update))
        self.logger.info("Rules to delete: %s", len(rules_to_delete))
        self.logger.info("Rules to create: %s", len(rules_to_create))

        return rules_to_update, api_rules, rules_to_delete, rules_to_create

    def process_updates(self):  # pylint: disable=R0914
        """Process any updates update process."""
        try:
            # Load local rules or initialize if empty
            local_rules = self.load_local_rules()

            # Get current API rules
            api_rules = self.get_all_rules()

            # If no local files exist, bootstrap the directory from the API
            if not local_rules:
                self.logger.info("No local rule files found, populating from current API rules")
                self.update_rules_file(api_rules)
                return True

            # Continue with normal update process
            rules_to_update, current_rules, rules_to_delete, rules_to_create = self.compare_rules(
                api_rules, local_rules
                )

            # Process creations first
            create_success = []
            create_failed = []

            for rule in rules_to_create:
                success, created_rule = self.create_rule_in_api(rule)
                if success and created_rule:
                    # Write the API-returned rule so the new ID is captured immediately
                    self.write_rule_file(created_rule)
                    create_success.append(created_rule)
                else:
                    create_failed.append(rule)
                    self.logger.error("Failed to create rule: %s", rule.get('name'))

            # Process deletions
            delete_success = []
            delete_failed = []

            for rule in rules_to_delete:
                if self.delete_rule_from_api(rule['id']):
                    # Remove the local file immediately on confirmed deletion
                    self.delete_rule_file(rule)
                    delete_success.append(rule['id'])
                else:
                    delete_failed.append(rule['id'])

            # Process updates
            update_success = []
            update_failed = []

            for rule in rules_to_update:
                success, updated_rule = self.update_rule_in_api(rule)
                if success and updated_rule:
                    # Overwrite local file with API-normalized state
                    self.write_rule_file(updated_rule)
                    update_success.append(updated_rule)
                else:
                    update_failed.append(rule)

            # Log results
            self.logger.info("Update summary:")
            self.logger.info("Successfully created: %s rules", len(create_success))
            self.logger.info("Failed to create: %s rules", len(create_failed))
            self.logger.info("Successfully deleted: %s rules", len(delete_success))
            self.logger.info("Failed to delete: %s rules", len(delete_failed))
            self.logger.info("Successfully updated: %s rules", len(update_success))
            self.logger.info("Failed to update: %s rules", len(update_failed))
            self.logger.info("Total rules tracked locally: %s",
                             len(create_success) + len(update_success)
                             + len(local_rules) - len(rules_to_create)
                             - len(delete_success))

            return len(create_failed) == 0 and len(update_failed) == 0 and len(delete_failed) == 0

        except Exception as e:
            self.logger.error("Error in update process: %s", str(e))
            print(traceback.format_exc())
            return False

    def update_rules_file(self, rules):
        """Write each rule to its own JSON file inside the rules directory.

        Files are named after the sanitized rule name (e.g. My_Rule.json).
        Any existing JSON files in the directory that no longer correspond to
        a current rule are removed so the directory stays in sync.
        """
        try:
            os.makedirs(self.rules_dir, exist_ok=True)

            # Build the set of filenames we expect to write
            expected_files = set()
            for rule in rules:
                file_path = self.rule_file_path(rule)
                filename = os.path.basename(file_path)
                expected_files.add(filename)
                with open(file_path, 'w', encoding="utf-8") as f:
                    json.dump(rule, f, indent=2)
                self.logger.debug("Wrote rule to %s", filename)

            # Remove stale rule files that are no longer in the current ruleset
            for existing_file in os.listdir(self.rules_dir):
                if existing_file.endswith(".json") and existing_file not in expected_files:
                    stale_path = os.path.join(self.rules_dir, existing_file)
                    os.remove(stale_path)
                    self.logger.info("Removed stale rule file: %s", existing_file)

            self.logger.info(
                "Successfully wrote %s rule file(s) to %s", len(rules), self.rules_dir
            )

        except Exception as e:
            self.logger.error("Error updating rules directory: %s", str(e))
            raise

    def update_rule_in_api(self, rule: Dict) -> Tuple[bool, Optional[Dict]]:  # noqa: C901
        """Update a single rule in the API.

        Returns (True, updated_rule) on success so the caller can immediately
        overwrite the local file with the API-normalized rule state.
        Returns (False, None) on failure.
        """
        try:
            # Define the allowed fields for update, including nested paths
            update_fields = {
                'id': None,
                'name': None,
                'description': None,
                'severity': None,
                'tactic': None,
                'technique': None,
                'search': {
                    'outcome': None,
                    'filter': None,
                    'lookback': None,
                    'trigger_mode': None
                }
            }

            # Helper function to get nested values
            def get_nested_value(source: Dict, field_path: List[str]) -> any:
                current = source
                for part in field_path:
                    if part not in current:
                        return None
                    current = current[part]
                return current

            # Helper function to set nested values
            def set_nested_value(target: Dict, field_path: List[str], value: any):
                current = target
                for part in field_path[:-1]:
                    current = current.setdefault(part, {})
                if value is not None:
                    current[field_path[-1]] = value

            # Build update payload recursively  # pylint: disable=R0912
            def build_payload(template: Dict,
                              source: Dict,
                              current_path: List[str] = None
                              ) -> Dict:
                if current_path is None:
                    current_path = []

                result = {}
                for key, value in template.items():
                    path = current_path + [key]
                    if value is None:
                        # Leaf node
                        source_value = get_nested_value(source, path)
                        if source_value is not None:
                            set_nested_value(result, path, source_value)
                    elif isinstance(value, dict):
                        # Handle nested search object specifically
                        if key == 'search' and 'search' in source and isinstance(
                            source['search'], dict
                            ):
                            # If source has nested search.search, use its contents
                            if 'search' in source['search']:
                                source_value = source['search']['search']
                            else:
                                source_value = source['search']

                            nested_result = {}
                            for subkey in value.keys():
                                if subkey in source_value:
                                    nested_result[subkey] = source_value[subkey]

                            if nested_result:
                                set_nested_value(result, path, nested_result)
                        else:
                            # Normal nested structure
                            nested_result = build_payload(value, source, path)
                            if nested_result:
                                set_nested_value(result, path, nested_result)
                return result

            update_payload = build_payload(update_fields, rule)

            response = self.falcon.update_rule(**update_payload)

            if response["status_code"] == 200:
                updated_rule = response["body"]["resources"][0]
                self.logger.info("Successfully updated rule %s", rule['id'])
                return True, updated_rule

            self.logger.error("Failed to update rule %s: %s", rule['id'], response)
            return False, None

        except Exception as e:
            self.logger.error("Error updating rule %s: %s", rule['id'], str(e))
            return False, None

    def create_rule_in_api(self, rule: Dict) -> Tuple[bool, Optional[Dict]]:
        """Create a new rule in the API.

        Returns (True, created_rule) on success so the caller can immediately
        write the new file with the API-assigned id and normalized fields.
        Returns (False, None) on failure.
        """
        try:
            # Define required fields including nested paths
            required_fields = {
                'name': None,
                'severity': None,
                'customer_id': None,
                'search': {
                    'filter': None,
                    'outcome': None,
                    'lookback': None,
                    'trigger_mode': None
                },
                'operation': {
                    'schedule': {
                        'definition': None
                    }
                },
                'status': None
            }

            # Helper function to get nested value
            def get_nested_value(data: Dict, path: List[str]) -> any:
                current = data
                for part in path:
                    if not isinstance(current, dict) or part not in current:
                        return None
                    current = current[part]
                return current

            # Helper function to check nested required fields
            def check_required_fields(template: Dict,
                                      data: Dict,
                                      current_path: List[str] = None
                                      ) -> List[str]:
                if current_path is None:
                    current_path = []

                missing = []
                for key, value in template.items():
                    path = current_path + [key]
                    if value is None:
                        # Check leaf node
                        if get_nested_value(data, path) is None:
                            missing.append('.'.join(path))
                    elif isinstance(value, dict):
                        # Check nested structure
                        nested_value = get_nested_value(data, path)
                        if nested_value is None:
                            missing.append('.'.join(path))
                        else:
                            missing.extend(check_required_fields(value, data, path))
                return missing

            # Check for missing required fields
            missing_fields = check_required_fields(required_fields, rule)
            if missing_fields:
                self.logger.error("Rule missing required fields: %s", missing_fields)
                return False, None

            # Remove any fields that shouldn't be included in creation
            create_payload = rule.copy()
            fields_to_remove = {'id', 'created_on', 'last_updated_on', 'deleted'}
            for field in fields_to_remove:
                create_payload.pop(field, None)

            response = self.falcon.create_rule(**create_payload)

            if response["status_code"] == 200:
                created_rule = response["body"]["resources"][0]
                self.logger.info("Successfully created rule: %s", rule.get('name'))
                return True, created_rule

            self.logger.error("Failed to create rule: %s", response)
            return False, None

        except Exception as e:
            self.logger.error("Error creating rule: %s", str(e))
            return False, None


def main():
    """Entry Point."""
    try:
        client = CorrelationRulesClient()
        success = client.process_updates()
        logging.info(
            "Script completed successfully" if success else "Script completed with some failures"
            )
        return 0 if success else 1
    except Exception as e:
        logging.error("Script failed: %s", str(e))
        return 1


if __name__ == "__main__":
    main()