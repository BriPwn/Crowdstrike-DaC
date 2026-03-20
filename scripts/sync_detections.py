r"""Sample to manage correlation rules as part of a detection as code pipeline.

██████╗ ██████╗ ███████╗███████╗██╗██████╗ ██╗ ██████╗
██╔══██╗██╔══██╗██╔════╝██╔════╝██║██╔══██╗██║██╔═══██╗
██████╔╝██████╔╝█████╗  ███████╗██║██║  ██║██║██║   ██║
██╔═══╝ ██╔══██╗██╔══╝  ╚════██║██║██║  ██║██║██║   ██║
██║     ██║  ██║███████╗███████║██║██████╔╝██║╚██████╔╝
╚═╝     ╚═╝  ╚═╝╚══════╝╚══════╝╚═╝╚═════╝ ╚═╝ ╚═════╝

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

                     Presidio Cybersecurity Practice

This solution leverages the CorrelationRules service collection to manage
CrowdStrike Correlation Rules as code, enabling version control and automated
deployment of detection rules.

REQUIRES
crowdstrike-falconpy v1.4.8 or greater    https://github.com/CrowdStrike/falconpy

Creation date: 02.28.2025 - Initial version, crowdstrikedcs@crowdstrike
Adapted by:    Presidio Cybersecurity Practice
"""
# pylint: disable=W0718,W0719
import json
import logging
import os
import traceback
from datetime import datetime, timezone, timedelta
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

        Always writes the dict as-is — callers are responsible for ensuring
        the rule contains the current API IDs before calling this method.
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

    def delete_rule_from_api(self, rule: Dict) -> bool:
        """Delete a rule from the API.

        Performs a live lookup by name immediately before the delete call so
        the request always carries the ID the API currently recognises, even
        if the ID changed since the last sync run.

        Returns True if successful.
        """
        try:
            name = rule.get("name", "")
            live = self._fetch_live_rule_by_name(name)
            if live:
                current_id = live["id"]
                if current_id != rule.get("id"):
                    self.logger.info(
                        "Refreshed ID for delete '%s': %s -> %s",
                        name, rule.get("id"), current_id
                    )
            else:
                # Rule not found in API — nothing to delete
                self.logger.warning(
                    "Rule '%s' not found in API; skipping delete", name
                )
                return True

            response = self.falcon.delete_rules(ids=current_id)

            if response["status_code"] == 200:
                self.logger.info("Successfully deleted rule '%s' (%s)", name, current_id)
                return True

            self.logger.error("Failed to delete rule '%s': %s", name, response)
            return False

        except Exception as e:
            self.logger.error("Error deleting rule '%s': %s", rule.get("name"), str(e))
            return False

    def _refresh_live_cache(self, rules: List[Dict] = None):
        """Build or rebuild the name-keyed live cache.

        If `rules` is supplied (e.g. the api_rules list already fetched in
        process_updates) the cache is built from that list with no extra API
        call.  If `rules` is None a fresh get_all_rules() call is made — used
        when a mid-run refresh is needed after creates, or as a fallback when
        the cache has not been initialised yet.

        Keyed by both raw and .strip()-ed name to tolerate trailing-space
        mismatches between local files and the API.
        """
        try:
            if rules is None:
                self.logger.debug("_refresh_live_cache: fetching fresh rule list from API")
                rules = self.get_all_rules()
            self._live_cache = {}
            for rule in rules:
                api_name = rule.get("name", "")
                self._live_cache[api_name] = rule
                self._live_cache[api_name.strip()] = rule
            self.logger.info("Live cache built with %s rules", len(rules))
            for n, r in self._live_cache.items():
                self.logger.debug("  cache entry: %r -> id=%s", n, r.get("id"))
        except Exception as e:
            self.logger.error("Failed to refresh live cache: %s", str(e))
            self._live_cache = {}

    def _fetch_live_rule_by_name(self, name: str) -> Optional[Dict]:
        """Return the current API record for a rule by name from the live cache.

        The cache is keyed by both raw and stripped names so trailing-space
        mismatches between the local file and the API are handled automatically.
        Returns None if the rule is not found in the cache.
        """
        if not hasattr(self, '_live_cache'):
            self.logger.warning("Live cache not initialised — calling _refresh_live_cache")
            self._refresh_live_cache()

        rule = self._live_cache.get(name) or self._live_cache.get(name.strip())
        if not rule:
            self.logger.warning("Live lookup found no rule matching name '%s'", name)
        return rule

    def compare_rules(self,
                      api_rules: List[Dict],
                      local_rules: List[Dict]
                      ) -> Tuple[List[Dict], List[Dict], List[Dict], List[Dict]]:
        """Compare API rules with local rules to identify updates, deletions, and creations.

        Matching is done by rule NAME (stable) rather than ID (changes on every
        modification) so the comparison is reliable even when IDs have drifted.

        Returns tuple of:
          (rules_to_update, current_rules, rules_to_delete, rules_to_create)

        rules_to_delete contains full rule dicts so callers can remove the
        corresponding local file after a confirmed API deletion.
        ID resolution for updates and deletes is handled inside each API method
        via a live _fetch_live_rule_by_name call immediately before the request.
        """
        self.logger.info("Starting rules comparison")

        # Index API rules by name — name is the only stable identifier
        api_rules_by_name = {rule['name']: rule for rule in api_rules}

        rules_to_create = []
        rules_to_update = []
        rules_to_delete = []

        for rule in local_rules:
            name = rule.get('name', '')

            if rule.get('deleted', False):
                # Only queue deletion if the rule still exists in the API
                if name in api_rules_by_name:
                    rules_to_delete.append(rule)
                    self.logger.info("Rule '%s' marked for deletion", name)
                else:
                    self.logger.info(
                        "Rule '%s' marked deleted but not found in API — skipping", name
                    )
                continue

            if not rule.get('id'):
                # No ID means it has never been pushed — create it
                rules_to_create.append(rule)
                self.logger.info("New rule to create: %s", name or 'unnamed')
                continue

            if name not in api_rules_by_name:
                self.logger.warning(
                    "Rule '%s' (id=%s) not found in API by name — skipping", name, rule.get('id')
                )
                continue

            api_rule = api_rules_by_name[name]
            if self.is_rule_different(rule, api_rule):
                self.logger.info("Rule '%s' has local changes — will update", name)
                rules_to_update.append(rule)

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

            # Get current API rules and build the live name cache used by
            # update and delete operations to resolve current IDs.
            # Pass the already-fetched list to avoid a redundant second API call.
            api_rules = self.get_all_rules()
            self._refresh_live_cache(rules=api_rules)

            # If no local files exist, bootstrap the directory from the API
            if not local_rules:
                self.logger.info("No local rule files found, populating from current API rules")
                self.update_rules_file(api_rules)
                return True

            # Continue with normal update process
            rules_to_update, current_rules, rules_to_delete, rules_to_create = (
                self.compare_rules(api_rules, local_rules)
            )

            # Process creations first
            create_success = []
            create_failed = []

            for rule in rules_to_create:
                success, created_rule = self.create_rule_in_api(rule)
                if success and created_rule:
                    # Write the API-returned rule so the assigned IDs are captured immediately
                    self.write_rule_file(created_rule)
                    create_success.append(created_rule)
                else:
                    create_failed.append(rule)
                    self.logger.error("Failed to create rule: %s", rule.get('name'))

            # Refresh the cache after creates so new rules are visible to the
            # delete and update loops if they reference the same names.
            if create_success:
                self._refresh_live_cache()

            # Process deletions — delete_rule_from_api does a live ID lookup internally
            delete_success = []
            delete_failed = []

            for rule in rules_to_delete:
                if self.delete_rule_from_api(rule):
                    # Remove the local file immediately on confirmed deletion
                    self.delete_rule_file(rule)
                    delete_success.append(rule.get("name"))
                else:
                    delete_failed.append(rule.get("name"))

            # Process updates — update_rule_in_api does a live ID lookup internally
            update_success = []
            update_failed = []

            for rule in rules_to_update:
                success, updated_rule = self.update_rule_in_api(rule)
                if success and updated_rule:
                    # Write back the API-returned rule so the file reflects current state
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

    def update_rule_in_api(self, rule: Dict) -> Tuple[bool, Optional[Dict]]:
        """Update a single rule in the API.

        Strategy: use the live API record as the base payload (guaranteed
        current ID and all required fields), then overlay only the user-editable
        fields from the local file.  This avoids the build_payload approach that
        was silently dropping fields the API requires (notifications, mitre_attack,
        operation, state, etc.) and was using stale IDs from the local file.

        Returns (True, updated_rule) on success, (False, None) on failure.
        """
        # Fields the user can edit in the local file that get overlaid onto
        # the live API record before sending.  Only these are ever written back
        # from the local file; everything else comes from the live record.
        USER_EDITABLE_FIELDS = (
            "name", "description", "severity", "tactic", "technique",
            "search", "status", "state", "notifications", "mitre_attack",
            "operation",
        )

        try:
            name = rule.get("name", "")

            # Always fetch the live record immediately before the call.
            live = self._fetch_live_rule_by_name(name)
            if not live:
                self.logger.error(
                    "Cannot update '%s': rule not found in live API state", name
                )
                return False, None

            # Log all three ID fields from the live record so we can see exactly
            # which identifier the API is returning and which one we should use.
            self.logger.info(
                "Live record for '%s': id=%s  rule_id=%s  executor_rule_id=%s",
                name,
                live.get("id"),
                live.get("rule_id"),
                live.get("executor_rule_id"),
            )

            # Start from the live record so IDs and system fields are always current.
            payload = dict(live)

            # Overlay user-editable fields from the local file.
            for field in USER_EDITABLE_FIELDS:
                if field in rule and rule[field] is not None:
                    payload[field] = rule[field]

            # The PATCH endpoint looks up the rule by the `id` field in the body,
            # but CrowdStrike's read/write planes use different identifiers.
            # `id` from get_rules_combined is the read-side record ID; the write
            # side may use `rule_id` instead.  Try rule_id as the body id first;
            # fall back to the native id if rule_id is absent.
            write_id = live.get("rule_id") or live.get("id")
            payload["id"] = write_id

            # Strip read-only / response-only fields the API rejects on update.
            for ro_field in (
                "created_on", "last_updated_on", "customer_id", "type",
                "api_client_id", "user_uuid", "user_id", "updated_by_api_client_id",
                "updated_by_user_uuid", "updated_by_user_id", "executor_rule_id",
                "rule_id", "template_id", "version", "next_execution_on",
                "last_execution", "status_msg", "deleted", "state",
            ):
                payload.pop(ro_field, None)

            # The PATCH endpoint only accepts status: "active" | "inactive".
            # The live record may carry transient values like "updating" that
            # CrowdStrike sets internally — normalise any non-accepted value to
            # whatever the local file intended, falling back to "active".
            valid_statuses = {"active", "inactive"}
            local_status = rule.get("status", "")
            live_status = live.get("status", "")
            if local_status in valid_statuses:
                payload["status"] = local_status
            elif live_status in valid_statuses:
                payload["status"] = live_status
            else:
                payload["status"] = "active"
                self.logger.info(
                    "Status '%s' is not valid for PATCH — defaulting to 'active' for '%s'",
                    live_status, name
                )

            # The API rejects a payload that contains both mitre_attack and
            # tactic/technique — they are mutually exclusive.  Prefer mitre_attack
            # (richer) and drop the flat fields when it is present.
            if payload.get("mitre_attack"):
                payload.pop("tactic", None)
                payload.pop("technique", None)

            # operation.start_on must be at least 15 minutes in the future on
            # updates as well as creates.  Strip it entirely on updates — the
            # rule is already scheduled and the API does not require a new
            # start_on to change other fields.
            if isinstance(payload.get("operation"), dict):
                payload["operation"].pop("start_on", None)
                # Also drop expiration_on — response-only field
                payload["operation"].pop("expiration_on", None)

            self.logger.info(
                "Sending update for '%s' with body id=%s status=%s",
                name, payload.get("id"), payload.get("status")
            )
            self.logger.debug("Update payload: %s", payload)

            response = self.falcon.update_rule(body=[payload])

            if response["status_code"] == 200:
                updated_rule = response["body"]["resources"][0]
                self.logger.info(
                    "Successfully updated '%s' -> new id=%s",
                    name, updated_rule.get("id")
                )
                return True, updated_rule

            # If rule_id didn't work, retry once with the native id.
            if response["status_code"] == 404 and write_id != live.get("id"):
                self.logger.warning(
                    "404 with rule_id=%s for '%s', retrying with native id=%s",
                    write_id, name, live.get("id")
                )
                payload["id"] = live.get("id")
                response = self.falcon.update_rule(body=[payload])
                if response["status_code"] == 200:
                    updated_rule = response["body"]["resources"][0]
                    self.logger.info(
                        "Retry with native id succeeded for '%s' -> new id=%s",
                        name, updated_rule.get("id")
                    )
                    return True, updated_rule

            self.logger.error(
                "Failed to update '%s' (id=%s): %s",
                name, payload.get("id"), response
            )
            return False, None

        except Exception as e:
            self.logger.error("Error updating rule '%s': %s", rule.get("name"), str(e))
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

            # The API requires operation.start_on to be at least 15 minutes in
            # the future. Static timestamps in rule files will always be stale,
            # so unconditionally set it to now + 16 minutes before every create.
            start_on = (datetime.now(timezone.utc) + timedelta(minutes=16)).strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )
            if isinstance(create_payload.get("operation"), dict):
                create_payload["operation"]["start_on"] = start_on
            else:
                create_payload["operation"] = {"start_on": start_on}
            self.logger.debug("Set operation.start_on to %s", start_on)

            self.logger.debug("Create payload: %s", create_payload)
            response = self.falcon.create_rule(body=[create_payload])

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