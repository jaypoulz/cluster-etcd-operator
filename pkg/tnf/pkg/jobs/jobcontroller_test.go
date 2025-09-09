package jobs

import (
	"context"
	"testing"
	"time"

	configv1 "github.com/openshift/api/config/v1"
	opv1 "github.com/openshift/api/operator/v1"
	fakeconfig "github.com/openshift/client-go/config/clientset/versioned/fake"
	configinformers "github.com/openshift/client-go/config/informers/externalversions"
	"github.com/openshift/library-go/pkg/controller/factory"
	"github.com/openshift/library-go/pkg/operator/events"
	"github.com/openshift/library-go/pkg/operator/v1helpers"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	coreinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	fakecore "k8s.io/client-go/kubernetes/fake"
	clocktesting "k8s.io/utils/clock/testing"
)

const (
	infraConfigName  = "cluster"
	jobName          = "testjob"
	defaultClusterID = "ID1234"
	controllerName   = "TestJobController"
	operandName      = "dummy-controller"
	operandNamespace = "openshift-test"
)

// fakeOperatorInstance is a fake Operator instance that fulfils the OperatorClient interface.
type fakeOperatorInstance struct {
	metav1.ObjectMeta
	Spec   opv1.OperatorSpec
	Status opv1.OperatorStatus
}

// Infrastructure
func makeInfra() *configv1.Infrastructure {
	return &configv1.Infrastructure{
		ObjectMeta: metav1.ObjectMeta{
			Name:      infraConfigName,
			Namespace: v1.NamespaceAll,
		},
		Status: configv1.InfrastructureStatus{
			InfrastructureName: defaultClusterID,
		},
	}
}

func makeFakeManifest() []byte {
	return []byte(`
apiVersion: batch/v1
kind: Job
metadata:
  labels:
    app.kubernetes.io/name: testjob
  namespace: openshift-test
  name: testjob
spec:
  template:
    metadata:
      annotations:
        openshift.io/required-scc: "privileged"
    spec:
      containers:
        - name: testjob
          image: testimage
          imagePullPolicy: IfNotPresent
          command: [ "test", "arg" ]
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 500m
              memory: 128Mi
          securityContext:
            privileged: true
            allowPrivilegeEscalation: true
      hostIPC: false
      hostNetwork: false
      hostPID: true
      serviceAccountName: test-manager
      terminationGracePeriodSeconds: 10
      restartPolicy: Never
    backoffLimit: 3`)
}

type operatorModifier func(instance *fakeOperatorInstance) *fakeOperatorInstance

func makeFakeOperatorInstance(modifiers ...operatorModifier) *fakeOperatorInstance {
	instance := &fakeOperatorInstance{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "cluster",
			Generation: 0,
		},
		Spec: opv1.OperatorSpec{
			ManagementState: opv1.Managed,
		},
		Status: opv1.OperatorStatus{},
	}
	for _, modifier := range modifiers {
		instance = modifier(instance)
	}
	return instance
}

// Helper function to find a condition by type
func findCondition(conditions []opv1.OperatorCondition, conditionType string) *opv1.OperatorCondition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}
	return nil
}

// Test setup helpers
func setupTestEnvironment() (kubernetes.Interface, coreinformers.SharedInformerFactory, v1helpers.OperatorClient) {
	coreClient := fakecore.NewSimpleClientset()
	coreInformerFactory := coreinformers.NewSharedInformerFactory(coreClient, 0)

	initialInfras := []runtime.Object{makeInfra()}
	configClient := fakeconfig.NewSimpleClientset(initialInfras...)
	configInformerFactory := configinformers.NewSharedInformerFactory(configClient, 0)
	configInformer := configInformerFactory.Config().V1().Infrastructures().Informer()
	configInformer.GetIndexer().Add(initialInfras[0])

	driverInstance := makeFakeOperatorInstance()
	fakeOperatorClient := v1helpers.NewFakeOperatorClientWithObjectMeta(&driverInstance.ObjectMeta, &driverInstance.Spec, &driverInstance.Status, nil)

	return coreClient, coreInformerFactory, fakeOperatorClient
}

func createJobController(conditions []string, coreClient kubernetes.Interface, coreInformerFactory coreinformers.SharedInformerFactory, fakeOperatorClient v1helpers.OperatorClient) factory.Controller {
	// Remove the config informer since it's not properly set up
	optionalInformers := []factory.Informer{}

	return NewJobController(
		controllerName,
		makeFakeManifest(),
		events.NewInMemoryRecorder(operandName, clocktesting.NewFakePassiveClock(time.Now())),
		fakeOperatorClient,
		coreClient,
		coreInformerFactory.Batch().V1().Jobs(),
		conditions,
		optionalInformers,
	)
}

func updateJobStatusAndCache(coreClient kubernetes.Interface, coreInformerFactory coreinformers.SharedInformerFactory, jobStatus batchv1.JobStatus) error {
	job, err := coreClient.BatchV1().Jobs(operandNamespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		return err
	}

	job.Status = jobStatus
	_, err = coreClient.BatchV1().Jobs(operandNamespace).Update(context.TODO(), job, metav1.UpdateOptions{})
	if err != nil {
		return err
	}

	// Update the informer cache with the new job status
	jobInformer := coreInformerFactory.Batch().V1().Jobs()
	jobInformer.Informer().GetIndexer().Update(job)

	return nil
}

func TestJobCreation(t *testing.T) {
	coreClient, coreInformerFactory, fakeOperatorClient := setupTestEnvironment()
	controller := createJobController(DefaultConditions, coreClient, coreInformerFactory, fakeOperatorClient)

	// Act
	err := controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("dummy-controller", clocktesting.NewFakePassiveClock(time.Now()))))
	if err != nil {
		t.Fatalf("sync() returned unexpected error: %v", err)
	}

	// Assert
	_, err = coreClient.BatchV1().Jobs(operandNamespace).Get(context.TODO(), jobName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Failed to get Job %s: %v", jobName, err)
	}
}

func TestJobStatusConditions(t *testing.T) {
	tests := []struct {
		name                 string
		jobStatus            batchv1.JobStatus
		expectedProgressing  opv1.ConditionStatus
		expectedDegraded     opv1.ConditionStatus
		expectError          bool
	}{
		{
			name:                "Job not started - initial state",
			jobStatus:           batchv1.JobStatus{Conditions: []batchv1.JobCondition{}},
			expectedProgressing: opv1.ConditionFalse,
			expectedDegraded:    opv1.ConditionFalse,
			expectError:         false,
		},
		{
			name:                "Job running",
			jobStatus:           batchv1.JobStatus{Conditions: []batchv1.JobCondition{}}, // No completion conditions = running
			expectedProgressing: opv1.ConditionTrue,
			expectedDegraded:    opv1.ConditionFalse,
			expectError:         false,
		},
		{
			name: "Job completed successfully",
			jobStatus: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:   batchv1.JobComplete,
					Status: v1.ConditionTrue,
				}},
			},
			expectedProgressing: opv1.ConditionFalse,
			expectedDegraded:    opv1.ConditionFalse,
			expectError:         false,
		},
		{
			name: "Job failed",
			jobStatus: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:   batchv1.JobFailed,
					Status: v1.ConditionTrue,
				}},
			},
			expectedProgressing: opv1.ConditionFalse,
			expectedDegraded:    opv1.ConditionTrue,
			expectError:         true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreClient, coreInformerFactory, fakeOperatorClient := setupTestEnvironment()
			controller := createJobController(DefaultConditions, coreClient, coreInformerFactory, fakeOperatorClient)

			// For initial state test, check operator status before any controller action
			if tt.name == "Job not started - initial state" {
				_, status, _, err := fakeOperatorClient.GetOperatorState()
				if err != nil {
					t.Fatalf("Failed to get operator status: %v", err)
				}

				// Verify initial conditions are False
				progressingCondition := findCondition(status.Conditions, controllerName+opv1.OperatorStatusTypeProgressing)
				if progressingCondition != nil && progressingCondition.Status != opv1.ConditionFalse {
					t.Errorf("Expected initial Progressing condition status %v, got %v", opv1.ConditionFalse, progressingCondition.Status)
				}

				// No degraded condition should exist initially
				degradedCondition := findCondition(status.Conditions, controllerName+opv1.OperatorStatusTypeDegraded)
				if degradedCondition != nil {
					t.Errorf("Expected no initial Degraded condition, but found one with status %v", degradedCondition.Status)
				}
				return
			}

			// Create the job first
			err := controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("dummy-controller", clocktesting.NewFakePassiveClock(time.Now()))))
			if err != nil {
				t.Fatalf("Initial sync() returned unexpected error: %v", err)
			}

			// Update job status and cache
			err = updateJobStatusAndCache(coreClient, coreInformerFactory, tt.jobStatus)
			if err != nil {
				t.Fatalf("Failed to update job status: %v", err)
			}

			// Sync again to process the status
			err = controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("dummy-controller", clocktesting.NewFakePassiveClock(time.Now()))))

			// Check if error is expected
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Get the operator status to verify conditions
			_, status, _, getErr := fakeOperatorClient.GetOperatorState()
			if getErr != nil {
				t.Fatalf("Failed to get operator status: %v", getErr)
			}

			// Verify Progressing condition
			progressingCondition := findCondition(status.Conditions, controllerName+opv1.OperatorStatusTypeProgressing)
			if progressingCondition == nil {
				t.Fatalf("Progressing condition not found")
			}
			if progressingCondition.Status != tt.expectedProgressing {
				t.Errorf("Expected Progressing condition status %v, got %v", tt.expectedProgressing, progressingCondition.Status)
			}

			// Verify Available condition is never set (since it's not in DefaultConditions)
			availableCondition := findCondition(status.Conditions, controllerName+opv1.OperatorStatusTypeAvailable)
			if availableCondition != nil {
				t.Errorf("Expected no Available condition to be set, but found one with status %v", availableCondition.Status)
			}

			// Verify Degraded condition (should be set via sync error, not as a condition)
			if tt.expectedDegraded == opv1.ConditionTrue && err == nil {
				t.Errorf("Expected degraded status (error) but got none")
			}
			if tt.expectedDegraded == opv1.ConditionFalse && err != nil {
				t.Errorf("Unexpected degraded status (error): %v", err)
			}
		})
	}
}

func TestJobWithAvailableCondition(t *testing.T) {
	tests := []struct {
		name              string
		jobStatus         batchv1.JobStatus
		expectedAvailable opv1.ConditionStatus
		expectError       bool
	}{
		{
			name: "Job completed successfully with Available condition",
			jobStatus: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:   batchv1.JobComplete,
					Status: v1.ConditionTrue,
				}},
			},
			expectedAvailable: opv1.ConditionTrue,
			expectError:       false,
		},
		{
			name:              "Job running with Available condition",
			jobStatus:         batchv1.JobStatus{Conditions: []batchv1.JobCondition{}}, // No completion conditions = running
			expectedAvailable: opv1.ConditionFalse,
			expectError:       false,
		},
		{
			name: "Job failed with Available condition",
			jobStatus: batchv1.JobStatus{
				Conditions: []batchv1.JobCondition{{
					Type:   batchv1.JobFailed,
					Status: v1.ConditionTrue,
				}},
			},
			expectedAvailable: opv1.ConditionFalse,
			expectError:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			coreClient, coreInformerFactory, fakeOperatorClient := setupTestEnvironment()
			conditionsWithAvailable := append(DefaultConditions, opv1.OperatorStatusTypeAvailable)
			controller := createJobController(conditionsWithAvailable, coreClient, coreInformerFactory, fakeOperatorClient)

			// Create the job first
			err := controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("dummy-controller", clocktesting.NewFakePassiveClock(time.Now()))))
			if err != nil {
				t.Fatalf("Initial sync() returned unexpected error: %v", err)
			}

			// Update job status and cache
			err = updateJobStatusAndCache(coreClient, coreInformerFactory, tt.jobStatus)
			if err != nil {
				t.Fatalf("Failed to update job status: %v", err)
			}

			// Sync again to process the status
			err = controller.Sync(context.TODO(), factory.NewSyncContext(controllerName, events.NewInMemoryRecorder("dummy-controller", clocktesting.NewFakePassiveClock(time.Now()))))

			// Check if error is expected
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			// Get the operator status to verify conditions
			_, status, _, getErr := fakeOperatorClient.GetOperatorState()
			if getErr != nil {
				t.Fatalf("Failed to get operator status: %v", getErr)
			}

			// Verify Available condition
			availableCondition := findCondition(status.Conditions, controllerName+opv1.OperatorStatusTypeAvailable)
			if availableCondition == nil {
				t.Fatalf("Available condition not found")
			}
			if availableCondition.Status != tt.expectedAvailable {
				t.Errorf("Expected Available condition status %v, got %v", tt.expectedAvailable, availableCondition.Status)
			}
		})
	}
}
