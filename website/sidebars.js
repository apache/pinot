/**
 * Copyright (c) 2017-present, Facebook, Inc.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

 module.exports = {
  docs:  [
    {
      type: 'category',
      label: 'About',
      items: [
        "about/what_is_pinot",
        "about/features_of_pinot",
        "about/who_use_pinot" 
      ],
    },
    {
      type: 'category',
      label: 'Administration',
      items: [
        "administration/running_locally",
        {
          type: 'category',
          label: 'Installation',
          items: [
            {
              type: 'category',
              label: 'Containers',
              items: [
                "administration/installation/containers/docker",
              ],
            },
            {
              type: 'category',
              label: 'Cloud',
              items: [
                  "administration/installation/cloud/on-premise",
                  "administration/installation/cloud/aws",
                  "administration/installation/cloud/gcp",
                  "administration/installation/cloud/azure"
              ],
            },
            {
              type: 'category',
              label: 'Operating Systems',
              items: [
                  "administration/installation/operating-systems/macos",
                  "administration/installation/operating-systems/ubuntu",
              ],
            },
          ],
        },        
      ],
    },
    {
      type: 'category',
      label: 'Components',
      items: [
        "components/broker",
        "components/cluster",
        "components/controller",
        "components/minion",
        "components/schema",
        "components/segments",
        "components/server", 
        "components/tables", 
        "components/tenants", 
      ],
    }
  ]
};
