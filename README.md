Tasmo
=========

#### What It Is
Tasmo is a system which enables application development on top of event streams and HBase. It’s functionality is similar to a materialized view in a relational database, where data is maintained at write time in the forms it is needed at read time for display and indexing.

#### Use Case
Tasmo is for significantly read heavy applications which display the same underlying data in multiple forms, where repeatedly performing the required selects and joins at read time can be prohibitively expensive. Tasmo allows the same flexibility of doing selects and joins at read time, but transfers the cost to write time. It also allows developers to think in terms of domain data, rather than HBase rows, columns, and keys.

#### How It Works
Tasmo based applications declare a set of events and views, where the events are the form data will be written, and the views are the multiple forms it will be read. Views constitute the composition and filtering of event data and are maintained at write time. Tasmo ingests streams of partial updates, and converts them to views by traversing a structural object graph derived from the declared event model. It then writes the event data in the forms required by the various view models.

#### Getting Started
Check out some Tasmo basics over at [the wiki](https://github.com/jivesoftware/tasmo/wiki).
