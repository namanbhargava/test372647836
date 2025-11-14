package com.example.eventrouler.service.impl;

import com.example.eventrouler.model.ClientRequest;
import com.example.eventrouler.model.Policy;
import com.example.eventrouler.model.PolicyCollection;
import com.example.eventrouler.service.EventRulerService;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import software.amazon.event.ruler.GenericMachine;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of EventRulerService using AWS Event Ruler library with GenericMachine
 * Uses GenericMachine<Policy> to store Policy objects directly as rule identifiers,
 * eliminating the need for separate template URL mapping
 * Loads policies from policies.json configuration file
 */
@Slf4j
@Service
public class EventRulerServiceImpl implements EventRulerService {

    private final GenericMachine<Policy> machine;
    private final ObjectMapper objectMapper;

    // Set of all matching fields used across all policies
    // This is populated during policy loading and used to extract only relevant fields
    private final Set<String> matchingFields = new java.util.HashSet<>();

    public EventRulerServiceImpl(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
        this.machine = GenericMachine.<Policy>builder().build();

        log.info("Initializing EventRulerService with AWS Event Ruler");
        loadPoliciesFromJson();

        log.info("Total unique matching fields across all policies: {}", matchingFields);

        // Diagnostic test - verify Event Ruler basics are working
        testEventRulerBasics();
    }

    private void testEventRulerBasics() {
        try {
            log.info("========== EVENT RULER DIAGNOSTIC TEST ==========");
            GenericMachine<String> testMachine = GenericMachine.<String>builder().build();

            // Create simple rule
            Map<String, List<String>> testRule = new java.util.HashMap<>();
            testRule.put("mode", java.util.Arrays.asList("online", "offline"));
            testRule.put("platform", java.util.Arrays.asList("web", "mobile"));

            testMachine.addRule("test-policy", testRule);
            log.info("Test rule added: {}", testRule);

            // Test 1: Exact match
            String test1 = "{\"mode\":\"online\",\"platform\":\"web\"}";
            List<String> result1 = testMachine.rulesForJSONEvent(test1);
            log.info("Test 1 - Exact match: {} -> Matched: {}", test1, result1.size());

            // Test 2: With extra field
            String test2 = "{\"mode\":\"online\",\"platform\":\"web\",\"id\":\"123\"}";
            List<String> result2 = testMachine.rulesForJSONEvent(test2);
            log.info("Test 2 - Extra field: {} -> Matched: {}", test2, result2.size());

            // Test 3: With nested object
            String test3 = "{\"mode\":\"online\",\"platform\":\"web\",\"ctx\":{\"id\":\"123\"}}";
            List<String> result3 = testMachine.rulesForJSONEvent(test3);
            log.info("Test 3 - Nested object: {} -> Matched: {}", test3, result3.size());

            log.info("========== END DIAGNOSTIC TEST ==========");
        } catch (Exception e) {
            log.error("Diagnostic test failed", e);
        }
    }

    /**
     * Loads policies from the policies.json file and adds them to the Event Ruler machine
     */
    private void loadPoliciesFromJson() {
        try {
            Resource resource = new ClassPathResource("policies.json");

            try (InputStream inputStream = resource.getInputStream()) {
                PolicyCollection policyCollection = objectMapper.readValue(inputStream, PolicyCollection.class);

                if (policyCollection == null || policyCollection.getItems() == null) {
                    log.warn("No policies found in policies.json");
                    return;
                }

                List<Policy> policies = policyCollection.getItems();

                log.info("Loading {} policies from policies.json", policies.size());

                // Add each policy to the Event Ruler machine
                // Using GenericMachine<Policy>, we pass the entire Policy object as the rule identifier
                for (Policy policy : policies) {
                    try {
                        log.info("Adding policy: {} (priority: {})", policy.getName(), policy.getPriority());
                        log.info("Policy rulesExpr type: {}", policy.getRulesExpr().getClass().getName());
                        log.info("Policy rulesExpr content: {}", policy.getRulesExpr());

                        // Collect all unique matching fields from this policy's rulesExpr
                        // This builds a set of ALL fields used across ALL policies for dynamic extraction
                        matchingFields.addAll(policy.getRulesExpr().keySet());

                        // Log each field in rulesExpr
                        policy.getRulesExpr().forEach((key, values) -> {
                            log.info("  Rule field '{}': {} (type: {})",
                                key, values, values.getClass().getName());
                            values.forEach(value ->
                                log.info("    Value: '{}' (length: {})", value, value.length())
                            );
                        });

                        // Add rule to Event Ruler machine with Policy object as identifier
                        // rulesExpr is already Map<String, List> - Jackson deserialized it automatically
                        machine.addRule(policy, policy.getRulesExpr());

                        log.info("Successfully loaded policy '{}' with rules: {}",
                                policy.getName(), policy.getRulesExpr());
                    } catch (Exception e) {
                        log.error("Error loading policy: {}", policy.getName(), e);
                    }
                }

                log.info("Successfully loaded {} policies", policies.size());
            }
        } catch (IOException e) {
            log.error("Error loading policies from policies.json", e);
            throw new RuntimeException("Failed to load policies from configuration file", e);
        }
    }

    @Override
    public Mono<Policy> evaluateEvent(ClientRequest clientRequest) {
        return Mono.fromCallable(() -> {
            log.info("Evaluating client request: mode={}, platform={}, id={}",
                    clientRequest.getMode(), clientRequest.getPlatform(), clientRequest.getId());

            try {
                // IMPORTANT: Event Ruler expects scalar event values; pattern arrays list allowed scalars.
                // Build event map with ONLY the fields defined in policy rules
                // This avoids nested objects and irrelevant fields that interfere with matching

                // Step 1: Convert ClientRequest to full map
                @SuppressWarnings("unchecked")
                Map<String, Object> fullMap = objectMapper.convertValue(
                        clientRequest,
                        new TypeReference<Map<String, Object>>() {}
                );

                // Step 2: Dynamically extract ONLY fields that are used in at least one policy
                // This automatically scales as you add new policies with new fields
                Map<String, Object> eventMap = new java.util.HashMap<>();
                for (String field : matchingFields) {
                    Object value = fullMap.get(field);
                    if (value != null) {
                        eventMap.put(field, value);
                    }
                }

                if (eventMap.isEmpty()) {
                    log.warn("No matching-relevant fields extracted from request; event map is empty. " +
                            "Available fields in request: {}, Required fields by policies: {}",
                            fullMap.keySet(), matchingFields);
                    return null;
                }

                // Convert clean event map to JSON string
                String jsonEvent = objectMapper.writeValueAsString(eventMap);
                log.info("Event JSON for matching (extracted {} of {} fields): {}",
                        eventMap.size(), matchingFields.size(), jsonEvent);

                // Find matching rules using rulesForJSONEvent (returns List<Policy> - complete Policy objects!)
                // This method parses the JSON and performs the matching against all loaded rules
                List<Policy> matchedPolicies = machine.rulesForJSONEvent(jsonEvent);

                log.info("Matched policies count: {}", matchedPolicies != null ? matchedPolicies.size() : 0);
                if (matchedPolicies != null && !matchedPolicies.isEmpty()) {
                    matchedPolicies.forEach(p -> log.info("  Matched: {}", p.getName()));
                }

                if (matchedPolicies == null || matchedPolicies.isEmpty()) {
                    log.warn("No matching rules found for event. JSON was: {}", jsonEvent);
                    return null;
                }

                // Get the first matched policy (if multiple policies match, we take the first)
                // Since policies are loaded by priority order, this gives us the highest priority match
                Policy matchedPolicy = matchedPolicies.get(0);

                log.info("Event matched policy: {} with URL: {}",
                        matchedPolicy.getName(), matchedPolicy.getConfig().getUrl());

                return matchedPolicy;
            } catch (Exception e) {
                log.error("Error evaluating event against rules", e);
                throw new RuntimeException("Failed to evaluate event", e);
            }
        }).subscribeOn(Schedulers.boundedElastic());
    }

}
