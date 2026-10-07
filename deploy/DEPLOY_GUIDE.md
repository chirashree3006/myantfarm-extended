# Cloud deployment guide

Goal: the same `docker compose` stack you run locally, running on a public
cloud VM. The shop is at `http://<public-ip>/` and the ops console at `/ops/`. Add a free
domain (section F) to get `https://` and Google sign-in.

**No card or bank details?** Use **GitHub Codespaces** (section G). It's free for every
GitHub account, with extra hours through the Student Pack.

**With a card:** Oracle Cloud *Always Free* Ampere A1 (4 vCPU / 24 GB RAM,
free forever). **Fallback** if Oracle says "Out of capacity" or rejects your
card: Azure for Students ($100 credit, no card, sign in with your VIT email),
Section B.

Sizing: the full stack needs about 3.5 GB RAM (two TinyLlama replicas at ~0.9 GB
each, plus 11 small containers). With less than 6 GB RAM the setup script
switches to lite mode (one TinyLlama replica).

---

## Step 0: Push the project to GitHub (from your laptop)

```powershell
cd C:\Users\HP\Documents\myantfarm-extended
git init
git add .
git commit -m "MyAntFarm-Extended: multi-agent + load balancing + cloud"
```

1. Go to https://github.com/new and create a repo called `myantfarm-extended`.
   Make it **Public**, which keeps the VM clone simple, and don't add a README.
2. Back in PowerShell:

```powershell
git branch -M main
git remote add origin https://github.com/<your-username>/myantfarm-extended.git
git push -u origin main
```

> `.gitattributes` keeps `*.sh` and `nginx.conf` with Linux line endings, so
> they work on the VM even though you committed from Windows.

---

## A. Oracle Cloud Always Free (recommended)

### A1. Create the account
1. https://signup.cloud.oracle.com: pick **Home Region = India South (Hyderabad)**
   or **India West (Mumbai)**. You can't change the home region later.
2. Card verification is a temporary hold. The Always Free resources never bill.

### A2. Create the VM
Menu → **Compute → Instances → Create instance**

| Field | Value |
|---|---|
| Name | `myantfarm` |
| Image | **Canonical Ubuntu 22.04** (or 24.04) |
| Shape | **Ampere → VM.Standard.A1.Flex**, **4 OCPU, 24 GB** (or 2 OCPU / 12 GB) |
| Networking | *Create new virtual cloud network* + *public subnet*, **Assign a public IPv4 address = Yes** |
| SSH keys | **Generate a key pair for me → Save private key** (e.g. `myantfarm.key`) |
| Boot volume | default (47 GB) is fine |

*Optional, fully automatic:* open **Show advanced options → Management →
Initialization script → Paste cloud-init script**, paste the whole of
`deploy/setup-vm.sh`, and change the line `REPO_URL="${1:-${REPO_URL:-}}"` to
`REPO_URL="https://github.com/<you>/myantfarm-extended.git"`. The VM then
installs and starts everything by itself. Skip A4.

Click **Create**. When it's *Running*, copy the **Public IP address**.

> "Out of capacity for shape VM.Standard.A1.Flex": try a different
> Availability Domain, try 2 OCPU / 12 GB, or retry later. If it keeps
> failing, use Section B.

### A3. Open ports 80, 443 and 8080 in the cloud firewall
Instance page → **Subnet** link → **Security Lists → Default Security List →
Add Ingress Rules**, then add two rules:

| Source CIDR | IP protocol | Destination port |
|---|---|---|
| `0.0.0.0/0` | TCP | `80` |
| `0.0.0.0/0` | TCP | `443` |
| `0.0.0.0/0` | TCP | `8080` |

(The setup script also opens these ports in Ubuntu's own iptables. Oracle
images block them by default, which is the #1 reason "it runs but I can't open
it".)

### A4. SSH in and run the setup script
On Windows PowerShell:

```powershell
# lock down the key file once, otherwise ssh refuses it
icacls C:\Users\HP\Downloads\myantfarm.key /inheritance:r /grant:r "$($env:USERNAME):(R)"
ssh -i C:\Users\HP\Downloads\myantfarm.key ubuntu@<PUBLIC-IP>
```

Now on the VM:

```bash
git clone https://github.com/<you>/myantfarm-extended.git
cd myantfarm-extended
sudo bash deploy/setup-vm.sh
```

It installs Docker, opens the firewall, adds swap if needed, builds the images,
pulls TinyLlama and waits until the model is loaded. This takes about 5–10
minutes the first time. It ends by printing:

```
Shop        : http://<PUBLIC-IP>/
Ops console : http://<PUBLIC-IP>/ops/
Website LB  : http://<PUBLIC-IP>:8080/
```

### A5. Verify
```bash
docker compose ps                         # all services "running"/"healthy"
curl -s localhost/lb/status               # nginx connection stats
curl -s localhost/api/multi/control/status | head -c 300
docker compose logs -f gateway            # watch upstream=... per request (LB evidence)
```
Open `http://<PUBLIC-IP>/ops/` and run: **Auth regression (paper) → Send traffic →
C2 vs C3x side by side**. Open `http://<PUBLIC-IP>/` in another tab and buy a record.

---

## B. Azure for Students (fallback, no credit card)

1. https://azure.microsoft.com/free/students: sign in with your VIT email.
2. Portal → **Virtual machines → Create**: Ubuntu Server 22.04, size **B2ms
   (2 vCPU, 8 GB)** or **B2s (4 GB, which runs in lite mode)**, authentication = SSH key
   (download the `.pem`), inbound ports: **HTTP (80), SSH (22)**.
3. After creation: VM → **Networking → Add inbound port rule** → port `8080`, TCP, Allow.
4. Then follow **A4 and A5** above with your `.pem` file and user `azureuser`.
5. **Stop (deallocate) the VM when you're not demoing** to save credit.

(AWS EC2 / GCP work the same way: Ubuntu 22.04, at least 4 GB RAM (t3.medium / e2-medium),
open 80 + 8080 in the security group/firewall, then A4. Free-tier t2.micro or
e2-micro with 1 GB is too small for TinyLlama.)

---

## C. Run the experiments against the cloud deployment

From your laptop (Python + `pip install -r loadtest/requirements.txt`):

```powershell
cd C:\Users\HP\Documents\myantfarm-extended\loadtest
# 1. paper reproduction (C1/C2/C3/C3x, DQ, T2U, ANOVA/t-tests)
python benchmark.py --base http://<PUBLIC-IP> --trials 20
# 2. load + fault tolerance (C2 vs C3x)
python resilience.py --base http://<PUBLIC-IP> --levels 1 4 8 16 --sla 60
# 3. raw load-balancer test on the website tier
python traffic.py --url http://<PUBLIC-IP>:8080 -n 1000 -c 50
```

Results land in `loadtest/results/` (`.md` table, `.csv`, `.json`, `.png` chart).
Use them in your report.

Run these on the VM itself; they need Docker:
```bash
cd ~/myantfarm-extended/loadtest && pip3 install httpx matplotlib
python3 resilience.py --docker --skip-load        # adds "LLM replica down" and "LLM tier outage"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"   # resource usage
```

---

## D. Operating it

| Task | Command (on the VM, in the project folder) |
|---|---|
| Update after a `git push` | `git pull && docker compose up -d --build` |
| Scale an agent tier | `docker compose up -d --scale agent-database=3` (Nginx picks new replicas up automatically) |
| Crash a component from the browser | ops console → Fault tolerance → click a service (503 for 60 s) |
| Kill a replica for real | `docker compose stop web2` → traffic keeps flowing via web1/web3 |
| Restart a replica | `docker compose start web2` |
| Logs | `docker compose logs -f coord1 agent-database` |
| Stop everything | `docker compose down` (add `-v` to delete the downloaded model) |

**Security note:** the ops console's control and chaos endpoints are public by design for the
demo (Nginx rate-limits them per IP). Shut the
VM down or `docker compose down` after your evaluation.

---

## E. Google sign-in (Google Cloud Console, about 5 minutes)

1. Go to https://console.cloud.google.com and create a project (`side-b-shop`).
2. **APIs & Services → OAuth consent screen**: choose **External**, set the app name to
   `Side B`, add your email as support and developer contact, and save. Under **Test users**,
   add the Google accounts that should be able to sign in, including your own and your teacher's.
3. **APIs & Services → Credentials → Create credentials → OAuth client ID**:
   * Application type: **Web application**
   * **Authorized JavaScript origins**, add every address you open the shop from:
     `http://localhost` (and `http://localhost:8000` if you changed the port), and for the
     cloud `https://<your-domain>` (see F). Google does not accept a bare IP address.
   * There's no redirect URI to add: the shop uses Google's sign-in button, which posts an ID
     token that the backend verifies.
4. Copy the **Client ID** (`…apps.googleusercontent.com`) into `.env`:
   ```
   GOOGLE_CLIENT_ID=1234567890-abc123.apps.googleusercontent.com
   ```
5. Restart the web tier: `docker compose up -d web1 web2 web3 web4`. The sign-in dialog now shows
   **Continue with Google**. New Google users get an account automatically. The backend checks
   the token's audience, issuer and verified email with Google before signing them in.

## F. HTTPS with a free domain (needed for Google sign-in on the cloud)

1. Go to https://www.duckdns.org, sign in, and create a subdomain (e.g. `sideb-joyit`). Set
   its IP to your VM's public IP.
2. On the VM, in the project folder:
   ```bash
   sed -i 's/^DOMAIN=.*/DOMAIN=sideb-joyit.duckdns.org/' .env
   sudo bash deploy/setup-vm.sh        # detects DOMAIN: Caddy gets a Let's Encrypt certificate
   ```
   This moves the gateway to port 8000 internally and puts Caddy on 80/443 in front of it.
3. Add `https://sideb-joyit.duckdns.org` to the Google client's **Authorized JavaScript
   origins** (E3).
4. Open `https://sideb-joyit.duckdns.org/` for the shop and `/ops/` for the ops console.
   Run the experiments with `--base https://sideb-joyit.duckdns.org`.

---

## G. GitHub Codespaces (free, no card, no bank details)

A codespace is a cloud Linux machine run by GitHub. The repo includes `.devcontainer/`, so it
builds and starts the whole stack (3 shop replicas, Nginx, agents, coordinators, 2× TinyLlama)
by itself.

1. Push the project to GitHub (step 0 above).
2. *Optional, more free hours:* get the **GitHub Student Developer Pack** at
   https://education.github.com/pack with your VIT email. It needs no card.
3. On your repo page: **Code → Codespaces → ⋯ → New with options → Machine type: 4-core
   (16 GB RAM) → Create codespace.**
4. Wait about 5–10 minutes on the first start, while it builds the images and downloads
   TinyLlama. The terminal prints:
   ```
   Shop        : https://<codespace-name>-80.app.github.dev/
   Ops console : https://<codespace-name>-80.app.github.dev/ops/
   ```
5. **Make it public for your teacher:** open the **Ports** tab, right-click port **80**, and
   choose **Port visibility → Public**. Anyone with the link can now open the shop.
6. **Google sign-in:** add `https://<codespace-name>-80.app.github.dev` to the Google client's
   Authorized JavaScript origins (section E), put the Client ID in `.env`, then run
   `docker compose up -d web1 web2 web3 web4`. The address stays the same for that codespace.
7. **Experiments:** run them in the codespace terminal:
   ```bash
   cd loadtest
   python benchmark.py --trials 10
   python resilience.py --docker          # includes LLM replica / LLM outage tests
   ```
   Download `loadtest/results/` from the file explorer (right-click → Download).
8. **Traffic-surge demo:** open `https://<codespace-name>-80.app.github.dev/backend/` (see README,
   "Traffic-surge demo").
9. **Update after a `git push` from your PC:** in the codespace terminal run
   `git checkout -- . && git pull && docker compose up -d --build`.

**Limits:** the codespace stops after 30 idle minutes. You can raise this to 4 hours under
GitHub → Settings → Codespaces → Default idle timeout. Free accounts get a fixed number of
core-hours a month (more with Student Pack/Pro), and a 4-core machine uses 4 per hour, so
**stop it when you're done** (Codespaces page → ⋯ → Stop). Starting it again brings everything
back up, with TinyLlama already downloaded.
