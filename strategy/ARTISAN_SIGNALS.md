# Artisan Web Redesign — Signal → Angle Mapping

## Scoring table

Scores are computed by `src/website-scorer.ts` (shared with restaurants pipeline).

| Signal | Points | Detection |
|--------|--------|-----------|
| `no_website` | +50 | Places API returns no website URL |
| `not_mobile` | +20 | Page lacks `<meta name="viewport">` |
| `no_ssl` | +15 | URL is http:// or HTTPS fetch fails |
| `old_builder` | +10 | Source mentions Wix, Jimdo, e-monsite, Weebly… |
| `slow_site` | +10 | Response time > 3 000 ms |
| `no_booking` | +10 | No booking/devis form keywords detected |
| `old_copyright` | +10 | Copyright year ≤ current year − 2 |
| `low_rating` | +5 | Google rating < 3.5 |
| `low_reviews` | +5 | Fewer than 50 Google reviews |

## Tier thresholds

| Tier | Score | Sequence |
|------|-------|----------|
| `hot` | ≥ 55 | 3 emails — Day 0, 3, 7 |
| `warm` | 35–54 | 2 emails — Day 0, 5 |
| `cold` | < 35 | 1 email — Day 0 |

Note: `no_website` alone (50) → warm. `no_website + low_rating` (55) → hot.

---

## Email angle decision tree

```
signals includes no_website    → angle: no_website
signals includes no_ssl        → angle: no_ssl
signals includes not_mobile    → angle: not_mobile
signals includes no_booking    → angle: no_devis
signals includes slow_site     → angle: slow_site
signals includes old_builder   → angle: old_builder
otherwise                      → angle: generic
```

---

## Angle briefs

### `no_website`
**Hook:** "J'ai cherché votre site avant de vous écrire — vos clients aussi le font avant d'appeler."
**Insight:** Artisans without a web presence lose quote requests to competitors who have one, even if those competitors are technically worse.
**CTA:** "Est-ce que c'est quelque chose qui vous manque ?"

### `no_ssl`
**Hook:** Browsers show a security warning before the client even reads the page. Fatal for online devis forms.
**Insight:** For an artisan asking clients to submit contact info online, an insecure site directly suppresses conversions.
**CTA:** "Ça vous pose problème en ce moment ?"

### `not_mobile`
**Hook:** "80 % des recherches de [trade] se font sur smartphone."
**Insight:** Local searches for tradespeople happen on mobile, on-site, or in an emergency. A site that doesn't render on mobile loses that client.
**CTA:** "C'est un problème que vous avez déjà remarqué ?"

### `no_devis` (mapped from `no_booking` signal)
**Hook:** "Un formulaire devis en ligne = plus de leads sans décrocher le téléphone."
**Insight:** Clients plan renovations at night. An online devis form captures intent at 11pm instead of losing it.
**CTA:** "C'est quelque chose qui manque chez vous ?"

### `slow_site`
**Hook:** "3 secondes de chargement et le client appelle le concurrent."
**Insight:** The 3-second rule is especially sharp for artisan searches — the client is often under time pressure and will call whoever loads first.
**CTA:** "Vous avez remarqué un taux de rebond élevé ?"

### `old_builder`
**Hook:** "Un template Wix limite vos devis en ligne et votre référencement local."
**Insight:** Wix/Jimdo sites have limited structured data, poor Core Web Vitals, and constrained form options — all critical for local artisan SEO.
**CTA:** "C'est quelque chose qui vous préoccupe ?"

### `generic`
**Hook:** Low-friction opener for warm leads with no dominant signal.
**CTA:** "Est-ce que ça vous parle ?"

---

## Writing rules

- **French only.** No anglicisms unless unavoidable.
- **Subject:** max 8 words, all lowercase, includes company name.
- **Body:** max 80 words, plain text, no HTML, no bullet points.
- **Reference** the specific detected signal — never generic praise.
- **CTA:** one open question only — no calls to schedule a demo, no links.
- **Sign** with first name only ("Paul"). No last name, no title.
- **Never** mention "automatisé", "IA", "intelligence artificielle", or "pipeline".
- **Tone:** direct, brief, peer-to-peer. Not salesy.
