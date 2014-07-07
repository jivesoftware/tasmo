package com.jivesoftware.os.tasmo.lib;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.model.Views;
import java.util.Enumeration;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.PatternLayout;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 *
 * @author jonathan
 */
public class ModePathPrefixMerging extends BaseTest {

    @BeforeClass
    public void logger() {
        String PATTERN = "%t %m%n";

        Enumeration allAppenders = LogManager.getRootLogger().getAllAppenders();
        while (allAppenders.hasMoreElements()) {
            Appender appender = (Appender) allAppenders.nextElement();
            appender.setLayout(new PatternLayout(PATTERN));
        }
        //if (verbose) {
        LogManager.getLogger("com.jivesoftware.os.tasmo.lib").setLevel(Level.TRACE);
        LogManager.getLogger("com.jivesoftware.os.tasmo.lib.TasmoViewModel").setLevel(Level.TRACE);
//            LogManager.getLogger("com.jivesoftware.os.tasmo.lib.concur.ConcurrencyAndExistanceCommitChange").setLevel(Level.TRACE);
//            LogManager.getLogger("com.jivesoftware.os.tasmo.reference.lib.ReferenceStore").setLevel(Level.TRACE);
//            LogManager.getLogger("com.jivesoftware.os.tasmo.view.reader.service.writer.WriteToViewValueStore").setLevel(Level.TRACE);
//        } else {
//            LogManager.getRootLogger().setLevel(Level.OFF);
//        }
    }

    @Test (dataProvider = "tasmoMaterializer", invocationCount = 1, singleThreaded = true)
    public void testModePathPrefixMerging(TasmoMaterializerHarness t) throws Exception {
        String viewClassName1 = "V1";
        String viewClassName2 = "V2";
        String viewClassName3 = "V3";
        String originalAuthor = "originalAuthor";
        String lastAuthor = "lastAuthor";
        String contentName = "contentName";
        Views views = TasmoModelFactory.modelToViews(
                viewClassName1 + "::" + originalAuthor + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName",
                viewClassName1 + "::" + lastAuthor + "::Version.ref_parent.ref.Content|Content.ref_lastAuthor.ref.User|User.userName",
                viewClassName2 + "::" + originalAuthor + "::Version.ref_parent.ref.Content|Content.ref_originalAuthor.ref.User|User.userName",
                viewClassName2 + "::" + lastAuthor + "::Version.ref_parent.ref.Content|Content.ref_lastAuthor.ref.User|User.userName",
                viewClassName3 + "::" + contentName + "::Version.ref_parent.ref.Content|Content.name"
        );
        t.initModel(views);
        
        ObjectId originalAuthorId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "ted").build());
        ObjectId lastAuthorId = t.write(EventBuilder.create(t.idProvider(), "User", tenantId, actorId).set("userName", "wendy").build());
        ObjectId content1 = t.write(EventBuilder.create(t.idProvider(), "Content", tenantId, actorId)
                .set("contentName", "booya")
                .set("ref_originalAuthor", originalAuthorId)
                .set("ref_lastAuthor", lastAuthorId).build());
        ObjectId version1 = t.write(EventBuilder.create(t.idProvider(), "Version", tenantId, actorId).set("ref_parent", content1).build());

        ObjectNode view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName1, version1.getId()));
        System.out.println("view1:" + mapper.writeValueAsString(view));

        view = t.readView(tenantIdAndCentricId, actorId, new ObjectId(viewClassName2, version1.getId()));
        System.out.println("view2:" + mapper.writeValueAsString(view));

        t.addExpectation(version1, viewClassName1, originalAuthor, new ObjectId[]{version1, content1, originalAuthorId}, "userName", "ted");
        t.addExpectation(version1, viewClassName1, lastAuthor, new ObjectId[]{version1, content1, lastAuthorId}, "userName", "wendy");
        t.addExpectation(version1, viewClassName2, originalAuthor, new ObjectId[]{version1, content1, originalAuthorId}, "userName", "ted");
        t.addExpectation(version1, viewClassName2, lastAuthor, new ObjectId[]{version1, content1, lastAuthorId}, "userName", "wendy");
        t.assertExpectation(tenantIdAndCentricId);
        t.clearExpectations();

        Assert.assertNotNull(view);
    }
}
