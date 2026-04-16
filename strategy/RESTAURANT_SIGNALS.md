# Restaurant Web Redesign — Signal → Angle Mapping

## Scoring table

| Signal | Points | Detection |
|--------|--------|-----------|
| `no_website` | +50 | Places API returns no website URL |
| `not_mobile` | +20 | Page lacks `<meta name="viewport">` |
| `no_ssl` | +15 | URL is http:// or HTTPS fetch fails |
| `old_builder` | +10 | Source mentions Wix, Jimdo, e-monsite, Weebly, OVH Sites… |
| `slow_site` | +10 | Response time > 3 000 ms |
| `no_booking` | +10 | No booking keywords (réserver, OpenTable, TheFork, Zenchef…) |
| `old_copyright` | +10 | Copyright year ≤ current year − 2 |
| `low_rating` | +5 | Google rating < 3.5 |
| `low_reviews` | +5 | Fewer than 50 Google reviews |

## Tier thresholds

| Tier | Score | Sequence |
|------|-------|----------|
| `hot` | ≥ 60 | 3 emails — Day 0, 3, 7 |
| `warm` | 35–59 | 2 emails — Day 0, 5 |
| `cold` | < 35 | 1 email — Day 0 |

---

## Email angle decision tree

```
signals includes no_website    → angle: no_website
signals includes not_mobile    → angle: not_mobile
signals includes no_booking    → angle: no_booking
signals includes old_builder
  OR old_copyright             → angle: old_website
signals includes low_rating    → angle: low_rating
otherwise                      → angle: generic
```

---

## Angle briefs

### `no_website`
**Hook:** 8 out of 10 customers look up a restaurant online before going. No site = invisible.  
**Tone:** Opportunity framing, not criticism. Many owners are simply too busy, not unaware.  
**CTA:** "On peut avoir quelque chose en ligne en moins de 2 semaines."

### `not_mobile`
**Hook:** 70 % of restaurant searches happen on a phone. If the site is hard to read, people bounce.  
**Tone:** Data-driven. Anchor on the stat, then offer a quick fix.  
**CTA:** "On peut corriger ça rapidement avec une refonte légère."

### `no_booking`
**Hook:** Direct revenue loss — customers who want to book at 23 h go elsewhere if they can't.  
**Tone:** Urgency without alarm. Cite average uplift (20 % covers).  
**CTA:** "On intègre ça dans les sites qu'on crée."

### `old_website`
**Hook:** Old site = bad first impression, especially vs. competitors with modern sites.  
**Tone:** Direct but respectful. Mention a specific visible symptom (builder badge, year).  
**CTA:** "On fait des refontes rapides, sans tout réinventer."

### `low_rating`
**Hook:** A below-average rating affects foot traffic. Online presence is part of the fix.  
**Tone:** Sensitive — acknowledge without rubbing it in. Focus on what's controllable.  
**CTA:** "On aide les restaurants à travailler ça."

### `generic`
**Hook:** A few small changes to your online presence could fill more covers.  
**Tone:** Low-key, non-committal. Good for warm leads with no dominant signal.  
**CTA:** "Est-ce que ça vous parle ?"
