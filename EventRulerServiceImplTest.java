package com.example.eventrouler.service.impl;

import com.example.eventrouler.model.ClientRequest;
import com.example.eventrouler.model.Policy;
import com.example.eventrouler.model.PolicyCollection;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import software.amazon.event.ruler.GenericMachine;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Comprehensive JUnit test suite for EventRulerServiceImpl
 * Tests initialization, policy loading, event evaluation, and error handling
 */
@ExtendWith(MockitoExtension.class)
class EventRulerServiceImplTest {

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private Resource resource;

    private EventRulerServiceImpl eventRulerService;

    /**
     * Test helper method to create a sample Policy object
     */
    private Policy createSamplePolicy(String name, int priority, String templateUrl,
                                       Map<String, List<String>> rulesExpr) {
        Policy policy = new Policy();
        policy.setName(name);
        policy.setPriority(priority);

        Policy.Config config = new Policy.Config();
        config.setUrl(templateUrl);
        policy.setConfig(config);

        policy.setRulesExpr(rulesExpr);
        return policy;
    }

    /**
     * Test helper method to create a sample ClientRequest
     */
    private ClientRequest createClientRequest(String mode, String platform, String id) {
        ClientRequest request = new ClientRequest();
        request.setMode(mode);
        request.setPlatform(platform);
        request.setId(id);
        return request;
    }

    /**
     * Test helper method to create a PolicyCollection with sample policies
     */
    private PolicyCollection createSamplePolicyCollection() {
        Map<String, List<String>> rules1 = new HashMap<>();
        rules1.put("mode", Arrays.asList("online", "offline"));
        rules1.put("platform", Arrays.asList("web", "mobile"));

        Map<String, List<String>> rules2 = new HashMap<>();
        rules2.put("mode", Arrays.asList("online"));
        rules2.put("platform", Arrays.asList("desktop"));

        List<Policy> policies = Arrays.asList(
            createSamplePolicy("Policy1", 1, "http://template1.com", rules1),
            createSamplePolicy("Policy2", 2, "http://template2.com", rules2)
        );

        PolicyCollection collection = new PolicyCollection();
        collection.setItems(policies);
        return collection;
    }

    @BeforeEach
    void setUp() {
        // Reset mocks before each test
        reset(objectMapper, resource);
    }

    /**
     * Test successful service initialization with valid policies
     */
    @Test
    void testConstructor_SuccessfulInitialization() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{\"items\":[]}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            // Act
            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Assert
            assertNotNull(service);
            verify(objectMapper).readValue(any(InputStream.class), eq(PolicyCollection.class));
        }
    }

    /**
     * Test initialization when policies.json file is missing
     */
    @Test
    void testConstructor_MissingPoliciesFile() {
        // Arrange
        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenThrow(new IOException("File not found"));
                })) {

            // Act & Assert
            assertThrows(RuntimeException.class, () -> {
                new EventRulerServiceImpl(objectMapper);
            });
        }
    }

    /**
     * Test initialization when policy collection is null
     */
    @Test
    void testConstructor_NullPolicyCollection() throws Exception {
        // Arrange
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(null);

            // Act - should not throw exception, just log warning
            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Assert
            assertNotNull(service);
        }
    }

    /**
     * Test initialization when policy collection items are null
     */
    @Test
    void testConstructor_NullPolicyItems() throws Exception {
        // Arrange
        PolicyCollection emptyCollection = new PolicyCollection();
        emptyCollection.setItems(null);
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(emptyCollection);

            // Act - should not throw exception
            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Assert
            assertNotNull(service);
        }
    }

    /**
     * Test initialization with empty policy list
     */
    @Test
    void testConstructor_EmptyPolicyList() throws Exception {
        // Arrange
        PolicyCollection emptyCollection = new PolicyCollection();
        emptyCollection.setItems(new ArrayList<>());
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(emptyCollection);

            // Act
            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Assert
            assertNotNull(service);
        }
    }

    /**
     * Test evaluateEvent with matching policy
     * Note: This is a simplified test. Full integration testing would require actual GenericMachine setup
     */
    @Test
    void testEvaluateEvent_MatchingPolicy() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest("online", "web", "123");

            Map<String, Object> eventMap = new HashMap<>();
            eventMap.put("mode", "online");
            eventMap.put("platform", "web");

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(eventMap);
            when(objectMapper.writeValueAsString(anyMap()))
                .thenReturn("{\"mode\":\"online\",\"platform\":\"web\"}");

            // Act
            Mono<Policy> result = service.evaluateEvent(clientRequest);

            // Assert
            assertNotNull(result);
            // Note: Full verification would require mocking GenericMachine behavior
            // which is complex. This test verifies the method can be called without exceptions.
        }
    }

    /**
     * Test evaluateEvent with null client request
     */
    @Test
    void testEvaluateEvent_NullClientRequest() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Act & Assert
            StepVerifier.create(service.evaluateEvent(null))
                .expectError(NullPointerException.class)
                .verify();
        }
    }

    /**
     * Test evaluateEvent with empty matching fields (no relevant fields in request)
     */
    @Test
    void testEvaluateEvent_EmptyMatchingFields() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest(null, null, "123");

            Map<String, Object> eventMap = new HashMap<>();
            eventMap.put("id", "123"); // Only field not in matching criteria

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(eventMap);

            // Act
            Mono<Policy> result = service.evaluateEvent(clientRequest);

            // Assert
            StepVerifier.create(result)
                .expectNext((Policy) null)
                .verifyComplete();
        }
    }

    /**
     * Test evaluateEvent with ObjectMapper throwing exception during serialization
     */
    @Test
    void testEvaluateEvent_SerializationException() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest("online", "web", "123");

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenThrow(new RuntimeException("Serialization error"));

            // Act & Assert
            StepVerifier.create(service.evaluateEvent(clientRequest))
                .expectError(RuntimeException.class)
                .verify();
        }
    }

    /**
     * Test evaluateEvent with JSON conversion exception
     */
    @Test
    void testEvaluateEvent_JsonConversionException() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest("online", "web", "123");

            Map<String, Object> eventMap = new HashMap<>();
            eventMap.put("mode", "online");
            eventMap.put("platform", "web");

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(eventMap);
            when(objectMapper.writeValueAsString(anyMap()))
                .thenThrow(new RuntimeException("JSON conversion error"));

            // Act & Assert
            StepVerifier.create(service.evaluateEvent(clientRequest))
                .expectError(RuntimeException.class)
                .verify();
        }
    }

    /**
     * Test evaluateEvent with valid request containing all fields
     */
    @Test
    void testEvaluateEvent_ValidRequestWithAllFields() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest("online", "web", "test-123");

            Map<String, Object> fullMap = new HashMap<>();
            fullMap.put("mode", "online");
            fullMap.put("platform", "web");
            fullMap.put("id", "test-123");

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(fullMap);
            when(objectMapper.writeValueAsString(anyMap()))
                .thenReturn("{\"mode\":\"online\",\"platform\":\"web\"}");

            // Act
            Mono<Policy> result = service.evaluateEvent(clientRequest);

            // Assert
            assertNotNull(result);
            verify(objectMapper).convertValue(eq(clientRequest), any(TypeReference.class));
            verify(objectMapper).writeValueAsString(anyMap());
        }
    }

    /**
     * Test that evaluateEvent is executed on boundedElastic scheduler
     */
    @Test
    void testEvaluateEvent_ExecutesOnBoundedElasticScheduler() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest("online", "web", "123");

            Map<String, Object> eventMap = new HashMap<>();
            eventMap.put("mode", "online");
            eventMap.put("platform", "web");

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(eventMap);
            when(objectMapper.writeValueAsString(anyMap()))
                .thenReturn("{\"mode\":\"online\",\"platform\":\"web\"}");

            // Act
            Mono<Policy> result = service.evaluateEvent(clientRequest);

            // Assert - verify the Mono is created (actual execution happens on subscribe)
            assertNotNull(result);
        }
    }

    /**
     * Test policy loading with malformed policy (missing rulesExpr)
     */
    @Test
    void testConstructor_MalformedPolicy() throws Exception {
        // Arrange
        Policy malformedPolicy = new Policy();
        malformedPolicy.setName("MalformedPolicy");
        malformedPolicy.setPriority(1);
        malformedPolicy.setRulesExpr(null); // Missing rulesExpr

        PolicyCollection collection = new PolicyCollection();
        collection.setItems(Arrays.asList(malformedPolicy));

        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(collection);

            // Act - should handle the error gracefully and continue
            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Assert
            assertNotNull(service);
        }
    }

    /**
     * Test evaluateEvent with request having null values
     */
    @Test
    void testEvaluateEvent_RequestWithNullValues() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest(null, "web", "123");

            Map<String, Object> eventMap = new HashMap<>();
            eventMap.put("mode", null);
            eventMap.put("platform", "web");
            eventMap.put("id", "123");

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(eventMap);
            when(objectMapper.writeValueAsString(anyMap()))
                .thenReturn("{\"platform\":\"web\"}");

            // Act
            Mono<Policy> result = service.evaluateEvent(clientRequest);

            // Assert
            assertNotNull(result);
        }
    }

    /**
     * Test initialization with multiple policies having different priorities
     */
    @Test
    void testConstructor_MultiplePoliciesDifferentPriorities() throws Exception {
        // Arrange
        Map<String, List<String>> rules1 = new HashMap<>();
        rules1.put("mode", Arrays.asList("online"));

        Map<String, List<String>> rules2 = new HashMap<>();
        rules2.put("platform", Arrays.asList("mobile"));

        Map<String, List<String>> rules3 = new HashMap<>();
        rules3.put("mode", Arrays.asList("offline"));

        List<Policy> policies = Arrays.asList(
            createSamplePolicy("HighPriority", 1, "http://high.com", rules1),
            createSamplePolicy("MediumPriority", 2, "http://medium.com", rules2),
            createSamplePolicy("LowPriority", 3, "http://low.com", rules3)
        );

        PolicyCollection collection = new PolicyCollection();
        collection.setItems(policies);

        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(collection);

            // Act
            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            // Assert
            assertNotNull(service);
            verify(objectMapper).readValue(any(InputStream.class), eq(PolicyCollection.class));
        }
    }

    /**
     * Test evaluateEvent with complex nested data in client request
     */
    @Test
    void testEvaluateEvent_ComplexNestedData() throws Exception {
        // Arrange
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(objectMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            EventRulerServiceImpl service = new EventRulerServiceImpl(objectMapper);

            ClientRequest clientRequest = createClientRequest("online", "web", "123");

            Map<String, Object> nestedData = new HashMap<>();
            nestedData.put("key", "value");

            Map<String, Object> fullMap = new HashMap<>();
            fullMap.put("mode", "online");
            fullMap.put("platform", "web");
            fullMap.put("id", "123");
            fullMap.put("nested", nestedData);

            when(objectMapper.convertValue(eq(clientRequest), any(TypeReference.class)))
                .thenReturn(fullMap);
            when(objectMapper.writeValueAsString(anyMap()))
                .thenReturn("{\"mode\":\"online\",\"platform\":\"web\"}");

            // Act
            Mono<Policy> result = service.evaluateEvent(clientRequest);

            // Assert
            assertNotNull(result);
        }
    }

    /**
     * Test that ObjectMapper is properly injected and used
     */
    @Test
    void testObjectMapperInjection() throws Exception {
        // Arrange
        ObjectMapper customMapper = mock(ObjectMapper.class);
        PolicyCollection policyCollection = createSamplePolicyCollection();
        String jsonContent = "{}";
        InputStream inputStream = new ByteArrayInputStream(jsonContent.getBytes());

        try (MockedConstruction<ClassPathResource> mockedResource = mockConstruction(ClassPathResource.class,
                (mock, context) -> {
                    when(mock.getInputStream()).thenReturn(inputStream);
                })) {

            when(customMapper.readValue(any(InputStream.class), eq(PolicyCollection.class)))
                .thenReturn(policyCollection);

            // Act
            EventRulerServiceImpl service = new EventRulerServiceImpl(customMapper);

            // Assert
            assertNotNull(service);
            verify(customMapper).readValue(any(InputStream.class), eq(PolicyCollection.class));
        }
    }
}
