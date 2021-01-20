package org.prebid.pg.gp.server.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.io.Resources;
import io.vertx.core.json.Json;
import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.prebid.pg.gp.server.model.DeliveryTokenSpendSummary;
import org.prebid.pg.gp.server.model.LineItem;
import org.prebid.pg.gp.server.model.PbsHost;
import org.prebid.pg.gp.server.model.ReallocatedPlan;
import org.prebid.pg.gp.server.spring.config.app.AlgorithmConfiguration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class TargetMatchedBasedTokenReallocationTest {

    private static final String VENDOR = "vendor1";

    private static final String REGION = "us-east";

    private static final String BIDDER_CODE = "pgvendor1";

    private AlgorithmConfiguration config;

    private TargetMatchedBasedTokenReallocation reallocationAlgo;

    @BeforeEach
    void setUpBeforeEach() {
        config = new AlgorithmConfiguration();
        config.setNonAdjustableSharePercent(90);
        reallocationAlgo = new TargetMatchedBasedTokenReallocation(config);
    }

    @Test
    void shouldCalculateProperlyInSeries() throws Exception {
        config.setNonAdjustableSharePercent(50);
        List<PbsHost> hosts = pbsHosts("host1", "host2", "host3");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 30));
        stats.add(tokenSpendSummary("host3", "111", BIDDER_CODE, 50));
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output17.json")));

        hosts = pbsHosts("host1", "host2", "host3", "host4", "host5", "host6");
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output18.json")));

        hosts = pbsHosts("host4", "host5", "host6");
        stats.clear();
        stats.add(tokenSpendSummary("host4", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host5", "111", BIDDER_CODE, 30));
        stats.add(tokenSpendSummary("host6", "111", BIDDER_CODE, 50));
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output19.json")));
    }

    @Test
    void shouldHandleRestartsProperly() throws Exception {
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output13.json")));

        hosts = pbsHosts("host1", "host2", "host3", "host4");
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output14.json")));

        hosts = pbsHosts("host3", "host4");
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output15.json")));
    }

    @Test
    void shouldHandleRestartsProperlyWithStatsData() throws Exception {
        config.setNonAdjustableSharePercent(90);
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output13.json")));

        hosts = pbsHosts("host1", "host2", "host3", "host4");
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 20));
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output14.json")));

        stats.clear();
        hosts = pbsHosts("host3", "host4");
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host3", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host4", "111", BIDDER_CODE, 20));
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output15.json")));
    }

    @Test
    void shouldCalculateWhenSomePbsServersPaused() throws Exception {
        config.setNonAdjustableSharePercent(90);
        List<PbsHost> hosts = pbsHosts("host1", "host2", "host3", "host4");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        // System.out.println(Json.mapper.writeValueAsString(result));
        assertThat(result, equalTo(loadTestResult("output14.json")));

        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 0));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 0));
        stats.add(tokenSpendSummary("host3", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host4", "111", BIDDER_CODE, 80));
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output16.json")));
    }

    @Test
    void shouldCalculateWhenZeroTotalTargetMatched() throws Exception {
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        // reallocate regularly based on targetMatched of stats data
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 0));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 0));
        List<ReallocatedPlan> result = new ArrayList<>();
        // targetMatched are all 0s, allocateWithStats, generate fake plans per stats
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output10.json")));
        // reallocate again with targetMatched are all 0s, allocateWithStats
        List<ReallocatedPlan> result1 = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result1, equalTo(loadTestResult("output10.json")));
    }

    @Test
    void shouldCalculateWhenZeroTargetMatched() throws Exception {
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        // reallocate regularly based on targetMatched of stats data
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 0));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 5));
        List<ReallocatedPlan> result = new ArrayList<>();
        // some targetMatched are 0s, allocateWithStats, generate fake plans per stats
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        // reallocate again with some targetMatched are 0s, allocateWithStats
        List<ReallocatedPlan> result1 = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result1, equalTo(loadTestResult("output11.json")));
    }

    @Test
    void shouldCalculateForNewOrExpiredLineItem() throws  Exception {
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        activeLineItems.add(LineItem.builder().lineItemId("222").bidderCode(BIDDER_CODE).build());
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(new ArrayList<>(), result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output1.json")));

        // reallocate regularly based on targetMatched of stats data
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 30));
        stats.add(tokenSpendSummary("host1", "222", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "222", BIDDER_CODE, 20));
        List<ReallocatedPlan> result1 =
                reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result1, equalTo(loadTestResult("output2.json")));

        // new line item '333' comes in
        stats.add(tokenSpendSummary("host3", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host3", "222", BIDDER_CODE, 20));
        activeLineItems.add(LineItem.builder().lineItemId("333").bidderCode(BIDDER_CODE).build());
        List<ReallocatedPlan> result3 = reallocationAlgo.calculate(stats, result1, activeLineItems, hosts);
        assertThat(result3, equalTo(loadTestResult("output5.json")));

        // line item '333' expired
        hosts = pbsHosts("host1", "host2");
        activeLineItems.remove(activeLineItems.size() - 1);
        List<ReallocatedPlan> result4 = reallocationAlgo.calculate(stats, result3, activeLineItems, hosts);
        assertThat(result4, equalTo(loadTestResult("output6.json")));
    }

    @Test
    void shouldCalculateForNewOrDeadHost() throws  Exception {
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        activeLineItems.add(LineItem.builder().lineItemId("222").bidderCode(BIDDER_CODE).build());
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(new ArrayList<>(), result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output1.json")));

        // reallocate regularly based on targetMatched of stats data
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 30));
        stats.add(tokenSpendSummary("host1", "222", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "222", BIDDER_CODE, 20));
        List<ReallocatedPlan> result1 = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result1, equalTo(loadTestResult("output2.json")));

        // new host 'host3' comes in
        hosts = pbsHosts("host1", "host2", "host3");
        List<ReallocatedPlan> result2 = reallocationAlgo.calculate(stats, result1, activeLineItems, hosts);
        assertThat(result2, equalTo(loadTestResult("output3.json")));

        // host 'host3' dead
        hosts = pbsHosts("host1", "host2");
        List<ReallocatedPlan> result3 = reallocationAlgo.calculate(stats, result2, activeLineItems, hosts);
        assertThat(result3, equalTo(loadTestResult("output4.json")));
    }

    @Test
    void shouldCalculateWithoutStatsData() throws Exception {
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        activeLineItems.add(LineItem.builder().lineItemId("222").bidderCode(BIDDER_CODE).build());
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(new ArrayList<>(), result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output1.json")));

        // reallocate without stats data
        List<ReallocatedPlan> result1 = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result1, equalTo(loadTestResult("output1.json")));

        // new host3 comes in
        List<PbsHost> hosts2 = pbsHosts("host1", "host2", "host3");
        List<ReallocatedPlan> result2 = reallocationAlgo.calculate(stats, result1, activeLineItems, hosts2);
        assertThat(result2, equalTo(loadTestResult("output12.json")));

        // hosts inactive
        List<ReallocatedPlan> result3 = reallocationAlgo.calculate(stats, result2, activeLineItems, hosts);
        assertThat(result3, equalTo(loadTestResult("output1.json")));
    }

    @Test
    void shouldCalculateIfMissingStatsData() throws Exception {
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        activeLineItems.add(LineItem.builder().lineItemId("222").bidderCode(BIDDER_CODE).build());
        List<ReallocatedPlan> result = new ArrayList<>();
        result = reallocationAlgo.calculate(new ArrayList<>(), result, activeLineItems, hosts);
        assertThat(result, equalTo(loadTestResult("output1.json")));

        // reallocate based on targetMatched of stats data
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 30));
        stats.add(tokenSpendSummary("host1", "222", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "222", BIDDER_CODE, 20));
        List<ReallocatedPlan> result1 =
                reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        assertThat(result1, equalTo(loadTestResult("output2.json")));

        stats.clear();
        // missing stats data for line '222'
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 30));
        List<ReallocatedPlan> result2 =
                reallocationAlgo.calculate(stats, result1, activeLineItems, hosts);
        assertThat(result2, equalTo(loadTestResult("output7.json")));

        stats.clear();
        // partial missing stats data for for line '111', '222'
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "222", BIDDER_CODE, 30));
        List<ReallocatedPlan> result3 = reallocationAlgo.calculate(stats, result2, activeLineItems, hosts);
        assertThat(result3, equalTo(loadTestResult("output8.json")));
    }

    @Test
    void shouldCalculateWhenHasStatsDataFirst() {
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        activeLineItems.add(LineItem.builder().lineItemId("222").bidderCode(BIDDER_CODE).build());
        List<ReallocatedPlan> result = new ArrayList<>();
        // plan request handler first does the even distribution among hosts
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 30));
        stats.add(tokenSpendSummary("host1", "222", BIDDER_CODE, 20));
        stats.add(tokenSpendSummary("host2", "222", BIDDER_CODE, 20));
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
    }

    @Test
    void shouldNoShareMigrationWhenNonAdjustableSharePercentIs100() throws Exception {
        List<PbsHost> hosts = pbsHosts("host1", "host2");
        List<LineItem> activeLineItems = new ArrayList<>();
        activeLineItems.add(LineItem.builder().lineItemId("111").bidderCode(BIDDER_CODE).build());
        // reallocate regularly based on targetMatched of stats data
        List<DeliveryTokenSpendSummary> stats = new ArrayList<>();
        stats.add(tokenSpendSummary("host1", "111", BIDDER_CODE, 5));
        stats.add(tokenSpendSummary("host2", "111", BIDDER_CODE, 0));
        List<ReallocatedPlan> result = new ArrayList<>();
        // change config to make sure no migration happens
        config.setNonAdjustableSharePercent(100);
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        // no migration happens
        assertThat(result, equalTo(loadTestResult("output10.json")));
        result = reallocationAlgo.calculate(stats, result, activeLineItems, hosts);
        // no happens happens
        assertThat(result, equalTo(loadTestResult("output10.json")));
    }

    private List<PbsHost> pbsHosts(String... instanceIds) {
        List<PbsHost> hosts = new ArrayList<>();
        for (String instanceId : instanceIds) {
            hosts.add(PbsHost.builder().vendor(VENDOR).hostInstanceId(instanceId).region(REGION).build());
        }
        return hosts;
    }

    private DeliveryTokenSpendSummary tokenSpendSummary(
            String instanceId, String extLineItemId, String bidderCode, int targetMatched) {
        return DeliveryTokenSpendSummary.builder()
                .vendor(VENDOR)
                .region(REGION)
                .instanceId(instanceId)
                .lineItemId(String.format("%s-%s", bidderCode, extLineItemId))
                .extLineItemId(extLineItemId)
                .bidderCode(bidderCode)
                .summaryData(DeliveryTokenSpendSummary.SummaryData.builder().targetMatched(targetMatched).build())
                .build();
    }

    private List<ReallocatedPlan> loadTestResult(String fileName) throws Exception {
        String path = "target-matched-reallocation/" +  fileName;
        final File statsFile = new File(Resources.getResource(path).toURI());
        final String content = FileUtils.readFileToString(statsFile, "UTF-8");
        return Json.mapper.readValue(content, new TypeReference<List<ReallocatedPlan>>(){});
    }

}

