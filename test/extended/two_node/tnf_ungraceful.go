package two_node

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	g "github.com/onsi/ginkgo/v2"
	o "github.com/onsi/gomega"
	v1 "github.com/openshift/api/config/v1"
	"github.com/openshift/origin/test/extended/etcd/helpers"
	"github.com/openshift/origin/test/extended/util"
	exutil "github.com/openshift/origin/test/extended/util"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// var _ = g.Describe("[sig-etcd][apigroup:config.openshift.io][OCPFeatureGate:DualReplica][Suite:openshift/two-node] Two Node with Fencing etcd recovery UNGNS", func() {
var _ = g.Describe("[sig-etcd][apigroup:config.openshift.io] Two Node UNGNS with Fencing etcd recovery", func() {
	defer g.GinkgoRecover()

	var (
		oc                      = exutil.NewCLIWithoutNamespace("").AsAdmin()
		etcdClientFactory       *helpers.EtcdClientFactoryImpl
		leaderNode, crashedNode corev1.Node
		kubeClient              kubernetes.Interface
	)

	g.BeforeEach(func() {
		skipIfNotTopology(oc, v1.DualReplicaTopologyMode)

		nodes, err := oc.AdminKubeClient().CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		o.Expect(err).ShouldNot(o.HaveOccurred(), "Expected to retrieve nodes without error")

		// Select the first index randomly
		randomIndex := rand.Intn(len(nodes.Items))
		leaderNode = nodes.Items[randomIndex]
		// Select the remaining index
		crashedNode = nodes.Items[(randomIndex+1)%len(nodes.Items)]
		g.GinkgoT().Printf("Randomly selected %s (%s) to be ungracefully shut down, and %s (%s) to take the lead\n", crashedNode.Name, crashedNode.Status.Addresses[0].Address, leaderNode.Name, leaderNode.Status.Addresses[0].Address)

		kubeClient = oc.KubeClient()
		etcdClientFactory = helpers.NewEtcdClientFactory(kubeClient)

		g.GinkgoT().Printf("Ensure both nodes are healthy before starting the test\n")
		o.Eventually(func() error {
			return helpers.EnsureHealthyMember(g.GinkgoT(), etcdClientFactory, leaderNode.Name)
		}, time.Minute, 5*time.Second).ShouldNot(o.HaveOccurred(), "expect to ensure Node A healthy without error")

		o.Eventually(func() error {
			return helpers.EnsureHealthyMember(g.GinkgoT(), etcdClientFactory, crashedNode.Name)
		}, time.Minute, 5*time.Second).ShouldNot(o.HaveOccurred(), "expect to ensure Node B healthy without error")
	})

	g.It("Should support an ungraceful node shutdown", func() {
		// TODO: get cluster ID for later comparison
		// TODO: get target's membership ID for later comparison

		g.By(fmt.Sprintf("Shutting down %s ungracefully", crashedNode.Name))
		err := util.TriggerNodeRebootUngraceful(kubeClient, crashedNode.Name)
		o.Expect(err).To(o.BeNil(), "Failed to reboot", crashedNode.Name, err)

		g.By(fmt.Sprintf("Ensuring target %s is a unreachable (it might take a while)", crashedNode.Name))
		o.Eventually(func() error {
			return helpers.EnsureHealthyMember(g.GinkgoT(), etcdClientFactory, crashedNode.Name)
		}, 5*time.Minute, 5*time.Second).Should(o.HaveOccurred(), fmt.Sprintf("Node %s should have left the cluster", crashedNode.Name))

		g.By(fmt.Sprintf("Ensuring leader %s is a healthy voting member", leaderNode.Name))
		o.Eventually(func() error {
			return helpers.EnsureHealthyMember(g.GinkgoT(), etcdClientFactory, leaderNode.Name)
		}, 5*time.Minute, 5*time.Second).ShouldNot(o.HaveOccurred(), fmt.Sprintf("Node %s should have become leader", leaderNode.Name))

		g.By(fmt.Sprintf("Ensuring that %s added %s back as learner", leaderNode.Name, crashedNode.Name))
		o.Eventually(func() error {
			members, err := getMembers(etcdClientFactory)
			if err != nil {
				framework.Logf("can't get members")
				return err
			}
			if len(members) != 2 {
				return fmt.Errorf("Not enough members")
			}

			if started, learner, err := getMemberState(&leaderNode, members); err != nil {
				framework.Logf("can't get %s member state: error %v", leaderNode.Name, err)
				return err
			} else if !started || learner {
				return fmt.Errorf("Expected node: %s to be a started and voting member. Membership: %+v", leaderNode.Name, members)
			}

			// Ensure GNS node is unstarted and a learner member (i.e. !learner)
			if _, learner, err := getMemberState(&crashedNode, members); err != nil {
				framework.Logf("can't get %s member state: error %v", crashedNode.Name, err)
				return err
			} else if !learner {
				return fmt.Errorf("Expected node: %s to be a learning member. Membership: %+v", crashedNode.Name, members)
			}

			g.GinkgoT().Logf("membership: %+v", members)
			return nil
		}, 10*time.Minute, 15*time.Second).ShouldNot(o.HaveOccurred())

		g.By(fmt.Sprintf("Ensuring %s rejoins as learner", crashedNode.Name))
		o.Eventually(func() error {
			members, err := getMembers(etcdClientFactory)
			if err != nil {
				return err
			}
			if len(members) != 2 {
				return fmt.Errorf("Not enough members")
			}

			if started, learner, err := getMemberState(&leaderNode, members); err != nil {
				return err
			} else if !started || learner {
				return fmt.Errorf("Expected node: %s to be a started and voting member. Membership: %+v", leaderNode.Name, members)
			}

			if started, learner, err := getMemberState(&crashedNode, members); err != nil {
				return err
			} else if !started || !learner {
				return fmt.Errorf("Expected node: %s to be a started and learner member. Membership: %+v", crashedNode.Name, members)
			}

			g.GinkgoT().Logf("membership: %+v", members)
			return nil
		}, 10*time.Minute, 15*time.Second).ShouldNot(o.HaveOccurred())

		g.By(fmt.Sprintf("Ensuring %s node is promoted back as voting member", crashedNode.Name))
		o.Eventually(func() error {
			members, err := getMembers(etcdClientFactory)
			if err != nil {
				return err
			}
			if len(members) != 2 {
				return fmt.Errorf("Not enough members")
			}

			if started, learner, err := getMemberState(&leaderNode, members); err != nil {
				return err
			} else if !started || learner {
				return fmt.Errorf("Expected node: %s to be a started and voting member. Membership: %+v", leaderNode.Name, members)
			}

			if started, learner, err := getMemberState(&crashedNode, members); err != nil {
				return err
			} else if !started || learner {
				return fmt.Errorf("Expected node: %s to be a started and voting member. Membership: %+v", crashedNode.Name, members)
			}

			g.GinkgoT().Logf("membership: %+v", members)
			return nil
		}, 30*time.Minute, 15*time.Second).ShouldNot(o.HaveOccurred())
	})
})
