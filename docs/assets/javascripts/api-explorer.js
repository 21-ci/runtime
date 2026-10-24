/*
 * Mounts the Scalar API reference on pages that contain <div id="scalar-api" data-spec="…">.
 * Works with Material's instant navigation (document$ fires on every page switch) and loads
 * the Scalar bundle from jsDelivr only when such a page is opened.
 */
(function () {
  "use strict";

  var SCALAR_SRC = "https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.73.0";
  var loading = null;

  function loadScalar() {
    if (window.Scalar && window.Scalar.createApiReference) return Promise.resolve();
    if (loading) return loading;
    loading = new Promise(function (resolve, reject) {
      var s = document.createElement("script");
      s.src = SCALAR_SRC;
      s.async = true;
      s.onload = function () { resolve(); };
      s.onerror = function () { loading = null; reject(new Error("could not load " + SCALAR_SRC)); };
      document.head.appendChild(s);
    });
    return loading;
  }

  function isDark() {
    return document.body.getAttribute("data-md-color-scheme") === "slate";
  }

  function syncTheme() {
    // Scalar styles itself through these body classes.
    var dark = isDark();
    document.body.classList.toggle("dark-mode", dark);
    document.body.classList.toggle("light-mode", !dark);
  }

  var observer = null;

  function mount() {
    var el = document.getElementById("scalar-api");
    if (!el) {
      document.body.classList.remove("dark-mode", "light-mode");
      if (observer) { observer.disconnect(); observer = null; }
      return;
    }
    if (el.getAttribute("data-mounted")) return;
    el.setAttribute("data-mounted", "1");

    var spec = new URL(el.getAttribute("data-spec"), window.location.href).toString();
    loadScalar().then(function () {
      el.innerHTML = "";
      window.Scalar.createApiReference(el, {
        url: spec,
        layout: "modern",
        theme: "purple",
        hideDarkModeToggle: true,
        forceDarkModeState: isDark() ? "dark" : "light",
        withDefaultFonts: false,
        operationTitleSource: "path",
        defaultOpenAllTags: false,
        showDeveloperTools: "never",
        agent: { disabled: true },
        telemetry: false
      });
      syncTheme();
      if (!observer) {
        observer = new MutationObserver(syncTheme);
        observer.observe(document.body, { attributes: true, attributeFilter: ["data-md-color-scheme"] });
      }
    }).catch(function (err) {
      el.innerHTML =
        '<p class="rt-api-explorer__loading">The interactive explorer could not be loaded (' +
        String(err.message || err) +
        '). Download <a href="' + spec + '">openapi.json</a> or open <code>/docs</code> on your node.</p>';
    });
  }

  if (window.document$ && typeof window.document$.subscribe === "function") {
    window.document$.subscribe(mount);
  } else {
    document.addEventListener("DOMContentLoaded", mount);
  }
})();
