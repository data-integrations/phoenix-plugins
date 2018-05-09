# Whole File Source

[![cm-available](https://cdap-users.herokuapp.com/assets/cm-available.svg)](https://docs.cask.co/cdap/current/en/integrations/cask-market.html)
![cdap-batch-source](https://cdap-users.herokuapp.com/assets/cdap-batch-source.svg)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Join CDAP community](https://cdap-users.herokuapp.com/badge.svg?t=wrangler)](https://cdap-users.herokuapp.com?t=1)

Whole file source reads the entire file. It's highly recommended not
to use this plugin when the files are very large and splittable.

## Plugin Configuration

| Configuration | Required | Default | Description |
| :------------ | :------: | :----- | :---------- |
| **Input Path** | **N** | 1 | Specifies path to a file or collection of files. |

## Usage Notes

If a path specified is a directory, then one file is read at a time
and passed along, but if the path specifies a file, then that exact
file is read and passed along.

# Build

## Clone this repo
Clone the this repo to your local environment

```
  git clone https://github.com/hydrator/whole-file-reader.git whole-file-reader
```

## Build

To build your plugins:

    mvn clean package -DskipTests

The build will create a .jar and .json file under the ``target`` directory.
These files can be used to deploy your plugins.

## Deployment
You can deploy your plugins using the CDAP CLI:

    > load artifact <target/whole-file-source-<version>.jar> config-file <target/whole-file-source-<version>.json>

For example, if your artifact is named 'repartitioner-<version>:

    > load artifact target/whole-file-source-<version>.jar config-file target/whole-file-source-<version>.json

# Mailing Lists

CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from 
users, release announcements, and any other discussions that we think will be helpful 
to the users.

# License and Trademarks

Copyright © 2016-2017 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the 
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, 
either express or implied. See the License for the specific language governing permissions 
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.

Apache, Apache HBase, and HBase are trademarks of The Apache Software Foundation. Used with
permission. No endorsement by The Apache Software Foundation is implied by the use of these marks.

.. |(Hydrator)| image:: http://cask.co/wp-content/uploads/hydrator_logo_cdap1.png


