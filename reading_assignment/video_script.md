# Video Script — "Tracking Aviation's Footprint from the Sky"

**Paper:** Quadros, Snellen, Sun & Dedoussi (2022). *Global Civil Aviation Emissions Estimates for 2017–2020 Using ADS-B Data.* Journal of Aircraft, 59(6), 1394–1405.

**Target length:** ~13 minutes (~1,800 words at ~145 words/min)

| Block | Speaker | Content | Time |
|-------|---------|---------|------|
| 1 | Pedro | Introduction + the problem | 0:00 – 4:15 |
| 2 | Speaker B | The approach + key results | 4:15 – 8:30 |
| 3 | Speaker C | Critical reflection + conclusion | 8:30 – 13:00 |

`[SLIDE: …]` lines are suggestions for what to show on screen.

---

## Block 1 — PEDRO (0:00 – 4:15)

`[SLIDE: Title — paper title, authors, journal, group names]`

Hi everyone. In this video we're presenting our reading assignment on a paper called *"Global Civil Aviation Emissions Estimates for 2017 to 2020 Using ADS-B Data"*, by Flávio Quadros, Mirjam Snellen, Junzi Sun and Irene Dedoussi from TU Delft, published in the *Journal of Aircraft* in 2022.

`[SLIDE: Photo of an airplane + a live flight map]`

We picked this paper because it sits right where data-intensive platforms meet an environmental issue. Every commercial aircraft constantly broadcasts its position over a radio system called ADS-B. Thousands of receivers on the ground collect those messages, and the result is a huge, continuous stream of data about almost every flight on the planet. The question the paper asks is simple: can we use that stream to measure how much aviation pollutes, and where?

It's also very close to our own course project. We're building a pipeline that ingests OpenSky ADS-B data with Kafka, Spark and MongoDB to study European air traffic. So this paper is basically a look at what happens when you push that same kind of data all the way to a climate question.

`[SLIDE: Agenda — Problem · Approach · Results · Reflection]`

I'll start by explaining the problem the paper tackles. Then Speaker B will explain the authors' approach and their main results, and Speaker C will finish with our group's critical reflection.

`[SLIDE: "Aviation ≈ 2–3% of global CO₂ — and more once non-CO₂ effects are counted"]`

So, the problem. Aviation produces roughly two to three percent of global CO₂ emissions. That may sound small, but it's growing quickly and it's one of the hardest sectors to decarbonise. On top of that, planes emit at high altitude, where other pollutants like nitrogen oxides, NOx, and the contrails they form have extra warming effects. So aviation's total impact on the climate is bigger than its CO₂ alone suggests.

`[SLIDE: Top-down vs bottom-up diagram]`

The problem is that we don't actually measure these emissions very well. Most official numbers, from organisations like the International Energy Agency or ICAO, are what's called *top-down*. Basically, they look at how much jet fuel was sold in each country and turn that into emissions. That approach has three big weaknesses.

First, it's **slow**. These numbers usually come out a year or more after the fact.

Second, it has **no spatial detail**. A fuel sales figure doesn't tell you whether the emissions happened over the North Atlantic at eleven kilometres altitude or at ground level near an airport. Climate and air-quality models need exactly that information.

Third, the alternative *bottom-up* inventories, which model individual flights, have mostly relied on airline schedules or radar data. They're often proprietary, limited to one region, or simply not updated every year.

`[SLIDE: Empty airports, 2020 — "How much did emissions really drop?"]`

All of this became obvious in 2020. When COVID-19 grounded a huge part of the world's fleet, scientists wanted to know how much aviation emissions actually dropped, and there was no quick, open, detailed answer.

So the gap the paper wants to close is this: we need a global aviation emissions inventory that is **timely**, **spatially resolved**, and **transparent**. The authors' bet is that ADS-B data can deliver all three. Now Speaker B will explain how they did it.

---

## Block 2 — SPEAKER B (4:15 – 8:30)

`[SLIDE: Pipeline diagram — ADS-B → flight lists → trajectories → fuel → emissions → global grid]`

Thanks, Pedro. The authors built what's essentially a data pipeline with four stages.

**Stage one is the activity data.** They take flights recorded by crowdsourced networks of ADS-B receivers between 2017 and 2020. From all those observations they build flight lists: for each combination of aircraft type, origin airport and destination airport, how many times it was flown. So for example, "an Airbus A320 from Madrid to Paris, flown X times this month".

`[SLIDE: Great circle route + inefficiency]`

**Stage two is the trajectory.** For each route they model the flight along the *great circle*, the shortest path over the Earth's surface. Real flights never take that perfect path, so they add a fixed correction for routing inefficiency: 40.5 nautical miles plus about 3.9 percent of the distance. Cruise altitudes come from observed data, and they also include monthly average winds, since a headwind or tailwind changes fuel burn a lot.

`[SLIDE: BADA logo / fuel flow curve]`

**Stage three is fuel burn.** They simulate climb, cruise and descent with BADA, an aircraft performance model from EUROCONTROL, which tells you how much fuel a specific aircraft type burns in each phase of flight. For take-off, taxiing and landing near the airport, they use a standard "time-in-mode" approach, which assigns typical durations and thrust settings to each phase.

`[SLIDE: Table of pollutants — CO₂, H₂O, SOₓ, NOₓ, CO, HC, nvPM]`

**Stage four is emissions.** Some pollutants, like CO₂ and water vapour, are directly proportional to fuel: every kilogram of jet fuel produces about 3.16 kilograms of CO₂. Others, like NOx, carbon monoxide and unburnt hydrocarbons, depend on the engine and the flight conditions. For those they use the ICAO Engine Emissions Databank, which has measured values for each engine, and correct them for altitude. They also estimate soot particles, called non-volatile particulate matter, from engine smoke measurements.

`[SLIDE: openAVEM GitHub + 4TU dataset screenshot]`

The final output is a global, three-dimensional grid of monthly emissions. One important detail: the authors released the model as open-source software, called **openAVEM**, and published the gridded results as an open dataset. That ties into the paper's claim that this approach is *transparent*.

`[SLIDE: Big numbers — 2019: 297 Tg fuel · 937 Tg CO₂ · 4.62 Tg NOx]`

So what did they find? For 2019, the last normal year before the pandemic, the model estimates that civil aviation burned about **297 million tonnes of fuel**, producing about **937 million tonnes of CO₂** and **4.6 million tonnes of NOx**.

`[SLIDE: Bar chart 2017–2020 with the 2020 drop highlighted]`

The most striking result is 2020. Because of COVID-19, fuel burn was about **48 percent lower** than it would otherwise have been. That's roughly **463 million tonnes of CO₂** and **2.3 million tonnes of NOx** that were never emitted. To put that in perspective, 463 million tonnes is more than the yearly CO₂ emissions of most individual countries.

`[SLIDE: Global emissions map]`

Because the inventory is spatially resolved, they can also show where emissions are concentrated, mainly along the big corridors over North America, Europe, East Asia and the North Atlantic. They also track how the average emissions per kilogram of fuel change over time as the fleet changes.

`[SLIDE: Quote — "ADS-B is a viable source for timely and transparent emissions estimates"]`

The authors conclude that ADS-B data is a viable way to produce global emissions estimates in a *timely and transparent* way. So, do we agree with them? Speaker C will present our critical reflection.

---

## Block 3 — SPEAKER C (8:30 – 13:00)

`[SLIDE: "What we agree with" — ✓ Reuse of safety data ✓ Speed ✓ Validated ✓ Open code]`

Thanks. Overall, we think the main idea of the paper is convincing and important.

First, we really like the idea of **reusing data**. ADS-B was built for air-traffic safety, not for climate science. Turning it into an environmental sensor is a great example of how data platforms can serve society beyond their original purpose.

Second, **speed**. Producing a global inventory within months instead of years is a real advance. It let researchers measure the COVID shock almost in real time, and the same approach could track whether things like new aircraft or sustainable fuels actually make a difference.

Third, the results seem **robust**. A later, independent study called GAIA, from Imperial College London, used full satellite ADS-B trajectories, which is a more detailed method. It got a 2019 fuel estimate only about 5 percent lower, and almost exactly the same COVID drop: minus 48.2 percent versus minus 47.1 percent. When two very different methods agree that closely, that's a good sign.

And releasing the code and the data is good scientific practice. But that's also where our concerns start, and we have four of them.

`[SLIDE: "Our concerns" — 1. Transparency 2. Coverage bias 3. Under-used data 4. Emissions ≠ climate impact]`

**One: "transparent" is only partly true.** The code is open, but it depends on BADA, which you have to license from EUROCONTROL. And according to the follow-up GAIA study, the flight data came from a commercial flight-tracking platform. So an independent group can't fully reproduce the inventory using only public resources. We think the claim would be stronger with fully open alternatives, like the OpenAP performance model and the OpenSky Network, which is the open data source we use in our own project.

`[SLIDE: World map of ADS-B receiver coverage — dense in EU/US, sparse in Africa/oceans]`

**Two: coverage bias.** Ground receivers are crowdsourced, so they're dense in Europe and North America but sparse over oceans, Africa and parts of Asia. A flight that no receiver sees simply doesn't exist in the dataset. That partly explains why the paper's 297 million tonnes of fuel is below the IEA's top-down estimate of about 320. And this bias isn't neutral. It under-counts exactly the regions where air travel is growing fastest. So a "global" inventory is really most accurate for the Global North.

`[SLIDE: Real trajectory with holding pattern vs idealised great circle]`

**Three, and this one matters a lot to our group: the data is richer than the model.** ADS-B gives you the *real* path of every aircraft. But the method reduces everything to origin–destination counts and then re-flies each route as an idealised great circle with monthly average winds. That throws away what actually happened: detours around closed airspace or conflict zones, delays, and holding patterns where planes circle while waiting to land. GAIA showed that a fixed 4 to 6 percent inefficiency factor underestimates the distance actually flown in regions like Europe. In our own project we're detecting holding patterns from real trajectories. Those are exactly the inefficiencies you'd want to measure, because they're the ones better air-traffic management could actually remove.

`[SLIDE: Contrails photo + bar "CO₂ vs contrail cirrus forcing"]`

**Four: emissions aren't the same as climate impact.** The paper reports how many tonnes of each pollutant were emitted. But research by Lee and colleagues in 2021 suggests that contrails, the white trails behind planes, may cause more warming than aviation's CO₂ itself. And whether a contrail forms depends on exactly where and when a plane flies, which is precisely what ADS-B tells you. So stopping at emissions leaves the most policy-relevant question unanswered. On top of that, the soot particle estimates, which matter a lot for contrail formation, are likely underestimated according to later work.

`[SLIDE: "Our position"]`

So, to sum up our position. We **agree** with the main conclusion: ADS-B data *can* support fast, global, detailed emissions monitoring, and the COVID-19 results hold up. But we **disagree** with how strongly the paper presents the approach as transparent, and we think coverage gaps make the "global" picture less reliable for the regions that are hardest to observe. And we think the biggest opportunity is the one the paper doesn't fully use: real trajectories. The next step is to use what aircraft actually did, not idealised routes, so we can link emissions to specific inefficiencies that can be fixed.

`[SLIDE: Thank you + references]`

Thanks for watching!

---

### References (for the last slide)
1. Quadros, F. D. A., Snellen, M., Sun, J., & Dedoussi, I. C. (2022). Global civil aviation emissions estimates for 2017–2020 using ADS-B data. *Journal of Aircraft*, 59(6), 1394–1405. https://doi.org/10.2514/1.C036763
2. Teoh, R., et al. (2024). The high-resolution Global Aviation emissions Inventory based on ADS-B (GAIA) for 2019–2021. *Atmos. Chem. Phys.*, 24, 725–744.
3. Lee, D. S., et al. (2021). The contribution of global aviation to anthropogenic climate forcing for 2000 to 2018. *Atmospheric Environment*, 244, 117834.
4. Schäfer, M., et al. (2014). Bringing up OpenSky: A large-scale ADS-B sensor network for research. *Proc. IPSN*.
