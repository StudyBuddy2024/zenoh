//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use std::collections::HashSet;
use zenoh::{config::WhatAmI, scout, Config};

#[tokio::main]
async fn main() {
    // initiate logging
    zenoh::init_log_from_env_or("error");

    println!("Scouting for Zenoh nodes...");
    let receiver = scout(WhatAmI::Peer | WhatAmI::Router, Config::default())
        .await
        .unwrap();

    // Collect discovered nodes (only unique ones)
    let mut discovered_nodes = Vec::new();
    let mut seen_zids = HashSet::new();

    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while let Ok(hello) = receiver.recv_async().await {
            // Only add node if we haven't seen this zid before
            if seen_zids.insert(hello.zid()) {
                println!("Discovered node: {}", hello);
                discovered_nodes.push(hello);
            }
        }
    })
    .await;

    // stop scouting
    receiver.stop();

    println!("\nTotal unique nodes discovered: {}", discovered_nodes.len());

    // Now try to connect to each discovered node and query its metadata
    for hello in discovered_nodes {
        println!("\n--- Querying metadata for zid: {} ---", hello.zid());

        // Try to connect to this node using one of its locators
        if let Some(locator) = hello.locators().first() {
            let mut config = Config::default();
            config
                .connect
                .endpoints
                .set(vec![locator.clone().into()])
                .unwrap();

            // Enable adminspace to query metadata
            config.adminspace.set_enabled(true).unwrap();

            match zenoh::open(config).await {
                Ok(session) => {
                    println!("  Connected to: {}", locator);

                    // Query the admin space for metadata
                    let admin_path = format!("@/{}/{}", hello.zid(),
                        if hello.whatami() == WhatAmI::Router { "router" }
                        else if hello.whatami() == WhatAmI::Peer { "peer" }
                        else { "client" }
                    );

                    println!("  Querying admin space at: {}", admin_path);

                    // Query the full admin space path for this node
                    match session.get(&admin_path).await {
                        Ok(replies) => {
                            let mut metadata_found = false;
                            while let Ok(reply) = replies.recv_async().await {
                                match reply.result() {
                                    Ok(sample) => {
                                        println!("  Reply: key={}", sample.key_expr());

                                        // Try to decode the payload as JSON for better readability
                                        let payload_bytes = sample.payload().to_bytes();
                                        if let Ok(json_str) = std::str::from_utf8(&payload_bytes) {
                                            // Try to parse and pretty-print the JSON
                                            if let Ok(pretty_json) = serde_json::from_str::<serde_json::Value>(json_str) {
                                                println!("  Value (pretty):");
                                                if let Some(metadata) = pretty_json.get("metadata") {
                                                    println!("    Metadata: {}", serde_json::to_string_pretty(metadata).unwrap_or_default());
                                                } else {
                                                    println!("    {}", serde_json::to_string_pretty(&pretty_json).unwrap_or_default());
                                                }
                                            } else {
                                                // If parsing fails, just show the string
                                                println!("  Value: {}", json_str);
                                            }
                                        } else {
                                            // If not valid UTF-8, show hex
                                            println!("  Value (hex): {:?}", payload_bytes);
                                        }
                                    }
                                    Err(e) => {
                                        println!("  Error: {}", e);
                                    }
                                }
                                metadata_found = true;
                            }

                            if !metadata_found {
                                println!("  No metadata found (adminspace may not be enabled on remote node)");
                            }
                        }
                        Err(e) => {
                            println!("  Failed to query admin space: {}", e);
                        }
                    }

                    // Close the session
                    session.close().await;
                }
                Err(e) => {
                    println!("  Failed to connect to {}: {}", locator, e);
                }
            }
        } else {
            println!("  No locators available for this node");
        }
    }
}
