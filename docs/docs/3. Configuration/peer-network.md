---
title: Peer Network
description: Connect AltMount instances to share release health and reuse finished imports.
keywords: [altmount, peer network, peernet, sharing, privacy]
---

# Peer Network

The peer network lets AltMount instances share release health information and reuse work from finished imports. **Media always comes from your own Usenet providers**, and AltMount checks what it receives from peers locally.

The feature is **off by default**. To get started, enable it and either add an open-network peer or join a private network with an invite.

## 1. Enable the peer network

Open **Configuration → Peer Network** under **System**.

![Peer Network settings with the feature off by default](/images/peer-network/enable.png)

Turn on **Enable peer network** to reveal the settings. In **This node**:

- Leave **Accept incoming connections** off if you only want to connect to other peers. You can receive, publish and serve bundles over connections your instance opens.
- Turn it on if you want to **host a private network** or let other peers connect directly to you. The default **Listen port** is `8470`; make that TCP port reachable through your firewall and container port mapping.
- Set **Advertise address** to the reachable `host:port` other peers should use, if needed. A reverse proxy must pass TCP through without terminating TLS.

![Unsaved node settings showing incoming connections, listen port and advertise address](/images/peer-network/node-settings.png)

Click **Save Changes** to start the node. Settings take effect without a restart. The screenshots show the form before saving; your Node ID appears after the node starts.

## 2. Choose what to share

Each network has its own **Exposure level**. Start with **1 - Receive only** to try the feature without publishing your library.

| Level | What you receive | What you share |
|---|---|---|
| **0 - Off** | Nothing | Nothing |
| **1 - Receive only** | Release details, health information and bundles on request | No library records |
| **2 - Share metadata** | Same as level 1 | Release details and health results for your own healthy imports |
| **3 - Share bundles** | Same as level 1 | Same as level 2, plus finished-import layouts on request, including article IDs |

A **bundle** describes how an imported release is put together. Another instance can reuse that layout while fetching the actual articles from its own providers.

## 3. Connect to a network

### Option A: Add an open-network peer

The open network has no built-in peers or public directory. Ask another operator for a reachable peer address.

1. In **Open network**, choose an exposure level of **1** or higher.
2. Under **Peer addresses**, enter the address as `host:port` and click **Add**. Do not include `http://`, `https://` or a path.
3. Leave **Minimum corroboration** at `3` to start with. This is how many distinct nodes must agree before a release is shown as healthy on the open network; nodes in the same network range count as one.
4. Click **Save Changes**. AltMount connects to the addresses you added and can discover more peers through them.

![Open network setup with Receive only selected and an example peer address ready to add](/images/peer-network/open-network.png)

`peer.example.com:8470` in the screenshot is a placeholder. Replace it with an address from another operator. Private LAN addresses and loopback addresses such as `localhost` cannot be used on the open network.

**Use offered layouts when importing** is optional and off by default. Turning it on lets normal NZB imports skip archive analysis when at least two publishers in different network ranges offer an identical bundle. Verification uses sampling. This switch does not control playing peer releases in Stremio.

### Option B: Join a private network

After enabling and saving the peer network:

1. Find **Private networks → Join network**.
2. Paste the invite link from the host.
3. If the host uses a LAN address, enable **Allow private network addresses (LAN)** for this trusted network.
4. Click **Join**.
5. Choose the network's exposure level and click **Apply level**.

Invite links are secrets and each works once. Private-network actions use their own buttons and apply immediately; they do not need the page's **Save Changes** button. You can leave the open network at level **0** when using only private networks.

### Option C: Host a private network

1. Turn on **Accept incoming connections**, configure a reachable address and port, and click **Save Changes**.
2. Under **Private networks → Create network**, enter a name and click **Create**.
3. Check the new network's **Host address** and save it if you change it. This address is included in new invites.
4. For a LAN network, turn on **Allow private addresses**. Members must also allow LAN addresses when joining.
5. Create an invite in the network's **Invites** section and give the link to the intended member.
6. Choose your exposure level and click **Apply level**.

For two instances on one machine, use the machine's **LAN IP** and different listen ports. Loopback (`127.0.0.1` or `localhost`) is never allowed, including on private networks.

## 4. Control what is published

Under **Sharing exclusions**, add categories or indexers whose releases you do not want to publish, then click **Save Changes**. Matching ignores case and surrounding spaces.

![Sharing exclusions for categories and indexers, followed by Save Changes](/images/peer-network/sharing-exclusions.png)

Only completed imports that pass a local health check are published. Some releases cannot be served as bundles, including rclone-encrypted files, releases with known holes or local PAR2 patches, and password-protected releases whose password cannot safely be shared.

New exclusions remove matching records your node previously published on the next upkeep pass, usually within about ten minutes. Copies already received by other nodes remain until they expire.

## What happens after connecting?

- **Health checks:** recent evidence from compatible providers can shorten some routine checks on the open network or postpone one on a private network. Your own checks still decide file health. New imports, manual checks and files with known problems still get local checks.
- **Imports:** eligible peer bundles can reuse a finished import's layout. AltMount verifies it locally and requires a full health check before accepting the import. Keep health monitoring enabled to receive bundles. If a bundle fails during an NZB import, AltMount falls back to importing your NZB normally.
- **Stremio:** peer releases with a fetchable bundle can appear as **“Peer network • verified on play”**. Playing one fetches and verifies the bundle before serving media from your own providers. **“Healthy on the peer network”** means enough qualifying peers reported healthy checks; it is not a guarantee.

## Privacy

Peers can see your **Node ID and IP address**. At level 1, requesting a bundle reveals that request to the peer serving it. At levels 2 and 3, published records reveal which healthy releases you have, their check times and recognised provider backbone names. At level 3, bundles also share article IDs and file names.

Provider credentials and hosts, API keys, local paths, ARR and indexer names, and rclone secrets are never shared. Category and indexer exclusions are kept locally.

## Troubleshooting

| Problem | What to check |
|---|---|
| No peers appear | Save your settings, choose level 1 or higher, and add a real reachable peer or join with an invite. A new install starts with no peers. |
| A LAN peer will not connect | Use a private network and allow private addresses on both nodes. Use a LAN IP rather than loopback. |
| Cannot create a private network | Enable **Accept incoming connections** and save first. The node must be running. |
| No healthy labels appear | Evidence must be recent and match your providers. Open-network nodes must have been verified for at least 24 hours and meet minimum corroboration. |
| A release is not shared | Check **Activity**, sharing exclusions and local health results. Health attestations also need a recognised provider backbone. |
| A peer cannot be reached | Check its address, firewall, TCP port mapping and the last contact error in the peer table. |

To stop all peer traffic, turn off **Enable peer network** and click **Save Changes**.
