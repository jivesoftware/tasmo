package com.jivesoftware.os.tasmo.local;

import com.jivesoftware.os.jive.utils.id.Id;
import com.jivesoftware.os.jive.utils.id.IdProvider;
import com.jivesoftware.os.jive.utils.id.ObjectId;
import com.jivesoftware.os.jive.utils.id.TenantId;
import com.jivesoftware.os.tasmo.configuration.ViewModelParser;
import com.jivesoftware.os.tasmo.event.api.write.Event;
import com.jivesoftware.os.tasmo.event.api.write.EventBuilder;
import com.jivesoftware.os.tasmo.event.api.write.EventWriter;
import com.jivesoftware.os.tasmo.event.api.write.EventWriterResponse;
import com.jivesoftware.os.tasmo.model.ViewBinding;
import com.jivesoftware.os.tasmo.view.reader.api.JsonViewProxyProvider;
import com.jivesoftware.os.tasmo.view.reader.api.ViewDescriptor;
import com.jivesoftware.os.tasmo.view.reader.api.ViewReader;
import com.jivesoftware.os.tasmo.view.reader.api.ViewResponse;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 *
 */
public class LocalMaterializationSystemTest {

    private List<ViewBinding> viewDefinition;

    public static interface SimpleContentView {

        public String subject();

        public String body();

        public User author();

        public interface User {

            public String userName();

            public long creationDate();
        }
    }

    @BeforeTest
    public void setUpModel() {

        String docViewAuthor = "SimpleContentView::authorInfo::Content.author.ref.User|User.userName,creationDate";
        String docViewContent = "SimpleContentView::contentInfo::Content.subject,body";

        viewDefinition = new ViewModelParser().parse(Arrays.asList(docViewAuthor, docViewContent));
    }

    @Test
    public void createAndUseMaterializer() throws Exception {
        LocalMaterializationSystem localMaterializationSystem = null;

        try {

            localMaterializationSystem =
                new LocalMaterializationSystemBuilder().build(viewDefinition);

            IdProvider idProvider = localMaterializationSystem.getIdProvider();

            TenantId tenantId = new TenantId("booya");
            Id actor = new Id(6_457);

            EventWriter writer = localMaterializationSystem.getWriter();
            ViewReader<ViewResponse> reader = localMaterializationSystem.getReader();

            Assert.assertNotNull(writer);
            Assert.assertNotNull(reader);

            long userCreated = System.currentTimeMillis();

            Event userEvent = EventBuilder.create(idProvider, "User", tenantId, actor).set("userName", "ted").set("creationDate", userCreated).build();
            Event contentEvent = EventBuilder.create(idProvider, "Content", tenantId, actor).set("subject", "awesome").set("body", "awesomer").
                set("author", userEvent.getObjectId()).build();

            EventWriterResponse response = writer.write(userEvent, contentEvent);
            List<ObjectId> objectIds = response.getObjectIds();

            Assert.assertNotNull(objectIds);
            Assert.assertEquals(objectIds.size(), 2);

            ObjectId viewId = new ObjectId("SimpleContentView", objectIds.get(1).getId());

            ViewResponse viewResponse = reader.readView(new ViewDescriptor(tenantId, actor, viewId));

            Assert.assertEquals(viewResponse.getStatusCode(), ViewResponse.StatusCode.OK);

            JsonViewProxyProvider proxyProvider = new JsonViewProxyProvider();
            SimpleContentView contentView = proxyProvider.getViewProxy(viewResponse.getViewBody(), SimpleContentView.class);

            Assert.assertNotNull(contentView);
            Assert.assertEquals(contentView.subject(), "awesome");
            Assert.assertEquals(contentView.body(), "awesomer");

            SimpleContentView.User user = contentView.author();
            Assert.assertNotNull(user);
            Assert.assertEquals(user.userName(), "ted");
            Assert.assertEquals(user.creationDate(), userCreated);

        } finally {
            if (localMaterializationSystem != null) {
                localMaterializationSystem.shutDown();
            }

        }

    }
}