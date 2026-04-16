# Signal → Email Angle Mapping

This file is loaded into every Claude Code session.
The orchestrator uses it to select the right email
angle based on what was detected about each lead.

---

## How to use

For each lead, signals_found[] contains a list of
detected signals. Pick the FIRST matching signal
from this list (priority order) and apply that angle.

---

## Signal map

### Signal: "naf_6920z"
Detected when: NAF code = 69.20Z (accounting firm)
Angle: reference the compliance burden angle
Subject formula: "question sur [specific pain] chez [company]"
Hook: open with a specific operational problem
  accountants face (deadline pressure, client volume,
  admin overhead) — not a generic compliment.

### Signal: "small_team" (headcount 10–19)
Detected when: trancheEffectifs = "11"
Angle: founder-to-founder, direct and brief
Framing: they wear many hats, time is scarce.
  One sentence on what you do. One question. Done.

### Signal: "mid_team" (headcount 20–49)
Detected when: trancheEffectifs = "12"
Angle: process + scale — they are growing and
  feeling friction. Reference that moment.

### Signal: "smtp_250" (verified email)
No special angle — just mark as tier_1 and
  send with confidence. No mention of verification.

### Signal: "no_spf"
Detected when: SPF record missing on their domain
Internal flag only — route to manual review before
  sending. Do not use as email angle.

### Signal: "generic_email" (contact@ or info@)
Angle: keep subject extremely specific to force
  a forward. "For the gérant of [company]" in
  subject line increases forward rate.

### Signal: "societe_com_found"
Detected when: dirigeant name sourced from societe.com
Angle: you can use their actual legal title
  (gérant, associé gérant, président) in the opening.
  More precise than "CEO". Increases trust.

### Fallback (no strong signal)
Use: mid_team angle as default.
Never send a fully generic email — always inject
  at minimum the company name and city.

---

## Email rules (apply to all angles)

- Plain text only. No HTML, no images, no tracking pixels.
- Subject: lowercase, under 8 words, no punctuation at end.
- Body: 3 sentences max before the CTA.
- CTA: one open question, not a link, not a calendar invite.
  Example: "Est-ce que c'est un sujet chez vous en ce moment ?"
- Sign-off: first name only. No title, no phone, no logo.
- Never mention "automated", "AI", "system", or "outbound".
- Never open with "Je me permets de vous contacter".
