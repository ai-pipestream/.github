# Quick Reference: Next Steps

## Immediate Actions Needed

### 1. Enable GitHub Pages
1. Go to: https://github.com/ai-pipestream/.github/settings/pages
2. Under "Build and deployment":
   - **Source**: Select "GitHub Actions"
3. Save changes

### 2. Merge This Pull Request
Once you merge this PR to `main`, the GitHub Actions workflow will automatically:
- Build the site
- Deploy it to GitHub Pages
- Make it available at https://ai-pipestream.github.io/.github/

### 3. Configure Your Domain (pipestream.ai)

You have two choices:

#### Choice A: Use Apex Domain (pipestream.ai)
Keep the `CNAME` file as-is with `pipestream.ai`

**Add these DNS records at your domain registrar**:
```
Type: A,     Name: @,     Value: 185.199.108.153
Type: A,     Name: @,     Value: 185.199.109.153
Type: A,     Name: @,     Value: 185.199.110.153
Type: A,     Name: @,     Value: 185.199.111.153
Type: CNAME, Name: www,   Value: ai-pipestream.github.io
```

#### Choice B: Use Subdomain (docs.pipestream.ai)
1. Edit `CNAME` file to contain `docs.pipestream.ai`
2. Add this DNS record:
   ```
   Type: CNAME, Name: docs, Value: ai-pipestream.github.io
   ```

### 4. SSL Certificate Setup

#### Recommended: Use GitHub's Automatic SSL ✅
- **Action**: Just wait for DNS to propagate (24-48 hours)
- **Then**: Go to Settings → Pages → Check "Enforce HTTPS"
- **Result**: Free, automatic, self-renewing SSL from Let's Encrypt

#### Alternative: Use Your Wildcard Certificate via Cloudflare
If you want to use your specific wildcard certificate:

1. **Sign up for Cloudflare** (free): https://cloudflare.com
2. **Add your domain** and update nameservers
3. **Add DNS record**:
   ```
   Type: CNAME
   Name: @ (or www, or docs)
   Target: ai-pipestream.github.io
   Proxy: ON (orange cloud)
   ```
4. **Upload your certificate**:
   - Go to SSL/TLS → Custom Certificates
   - Upload your .crt and .key files
5. **Configure SSL**:
   - SSL mode: "Full"
   - Always Use HTTPS: ON

### 5. Verify It Works

After DNS propagates:
- Visit your domain: https://pipestream.ai (or https://docs.pipestream.ai)
- Check for the lock icon (HTTPS working)
- Verify the content displays correctly

## Troubleshooting

**DNS not working?**
```bash
# Check DNS propagation
dig pipestream.ai
# or
nslookup pipestream.ai

# Check globally: https://dnschecker.org
```

**HTTPS not working?**
- Wait for DNS to fully propagate first
- Ensure "Enforce HTTPS" is enabled in Pages settings
- GitHub needs to verify domain ownership before issuing cert

**Page not deploying?**
- Check Actions tab: https://github.com/ai-pipestream/.github/actions
- Look for any failed workflow runs
- Ensure Pages is enabled in repository settings

## Timeline

- **Now**: Merge PR, enable Pages
- **5 minutes**: First deployment complete, site live at GitHub URL
- **24-48 hours**: DNS propagates, custom domain works
- **A few minutes after DNS**: HTTPS certificate issued automatically

## Need Help?

- Full documentation: See [SETUP.md](SETUP.md)
- Open an issue: https://github.com/ai-pipestream/.github/issues

---

**Summary**: Just enable GitHub Pages, merge this PR, configure DNS, and you're done! GitHub handles the SSL automatically.
