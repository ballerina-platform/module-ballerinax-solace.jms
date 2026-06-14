# Test broker — local Solace PubSub+

This directory holds the local Solace broker used by the integration tests
([docker-compose.yaml](./docker-compose.yaml)) and the queue/replay-log provisioning script
([init-solace.sh](./init-solace.sh)).

## Start / stop the broker

```bash
# from the repo root
docker compose -f ballerina/tests/resources/docker-compose.yaml up -d     # start
docker compose -f ballerina/tests/resources/docker-compose.yaml down      # stop + remove
```

Ports exposed to your machine:

| Port | Purpose |
|------|---------|
| `55554` | SMF messaging (the connector uses this — `smf://localhost:55554`) |
| `55003` | SMF compressed messaging |
| `8008`  | WebSocket messaging (the **Try Me!** panel in the broker UI) |
| `8080`  | Management UI / SEMP (`http://localhost:8080`, admin/admin) |

## Manually send & receive from the broker UI ("Try Me!")

The broker's **Try Me!** panel is a browser WebSocket client — it connects over `ws://`, **not** SMF.
Follow these steps exactly; the one common mistake is leaving the Message VPN blank.

1. Start the broker (above) and wait ~30 s for it to come up.
2. Open the manager at **`http://localhost:8080`** (use plain `http`, not `https`, so the browser
   allows the `ws://` connection). Log in with **admin / admin**.
3. In the left nav, click **Message VPNs**, then click into the **`default`** VPN.
   (Opening Try Me! from *inside* the VPN is what pre-fills the Message VPN field — opening it from
   the global view leaves it blank, which causes `Message VPN Not Allowed`.)
4. Open **Try Me!** (top nav of the VPN view).
5. In the connection settings confirm:
   - **Broker URL (messaging):** `ws://localhost:8008`
   - **Message VPN:** `default`  *(exactly, case-sensitive, no spaces)*
   - **Client Username:** `default`
   - **Password:** *(leave blank)*
6. Click **Connect** in **both** the Publisher and Subscriber sub-panels (each connects separately).
7. In the Subscriber, set the topic to `try-me/topic` and click **Subscribe**.
8. In the Publisher, set the topic to `try-me/topic`, type a message, and click **Publish** —
   it appears in the Subscriber panel.

### Troubleshooting

| Symptom | Cause / fix |
|---------|-------------|
| `Connection error` / cannot connect | `8008` not reachable — recreate the broker after a compose change: `docker compose -f ballerina/tests/resources/docker-compose.yaml up -d --force-recreate`. Use `ws://`, and open the UI over `http://` (not `https://`). |
| `Message VPN Not Allowed` | The Message VPN field is blank or wrong. Open Try Me! from inside the **`default`** VPN (step 3) and confirm the field reads exactly `default`. |
| Connects but `Topic subscription not allowed` | Use a simple topic such as `try-me/topic`. |

## Provision the test queues (only needed for the automated/connector tests)

The Try Me! panel does **not** need pre-provisioned queues. The integration tests and the
`examples/order-processing` flow do. Provisioning is automatic when you run the tests
(`./gradlew :solace-ballerina:test`); to do it by hand against a running broker:

```bash
./ballerina/tests/resources/init-solace.sh
```

The script waits for the broker to be ready and creates every test queue, topic-to-queue
subscription, and the replay log, failing loudly if any step does not succeed.
